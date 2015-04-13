(*
  PosgreSQL
  This module simplify request to PosgreSQL
*)

(******************************************************************************
******************************* Configuration *********************************
*******************************************************************************)

let _ = Random.self_init ()

module Lwt_thread = struct
  include Lwt
  include Lwt_chan
end
module Lwt_PGOCaml = PGOCaml_generic.Make(Lwt_thread)
module PGOCaml = Lwt_PGOCaml

module Table =
struct
  let content = "content"
  let tag = "tag"
  let link = "link"
end

module Conf =
struct
  let limit = 20
  let limit_coef = 2
  let mark_decimal = 3.
  let internal_limit = limit * limit_coef
  let mark_precision = Utils.Maths.power 10. mark_decimal
end

module Format =
struct

  module To_get =
  struct
    let content = ["content.content_uri"; "title"; "summary"]
    let link = ["link_id"; "origin_uri"; "target_uri";
                "nature"; "link.mark"; "user_mark"]
    let linked_content = ["link_id"; "nature"; "link.mark"; "user_mark";
                          "content.content_uri"; "title"; "summary"]
    let tag = ["tag_id"; "tag.content_uri"; "subject"; "tag.mark"]
  end

  module To_set =
  struct
    let content = ["content_uri"; "title"; "summary"]
    let link = ["origin_uri"; "target_uri"; "nature"; "mark"; "user_mark"]
    let tag = ["tag_id"; "content_uri"; "subject"; "mark"]
  end

end

(******************************************************************************
************************************ Utils ************************************
*******************************************************************************)

let (^^) a b = a ^ " " ^ b

module Pg =
struct

  let connect () = PGOCaml.connect ()
    (* ~host="127.0.0.1" *)
    (* ~user="api" *)
    (* ~password="1234" *)
    (* ~database="pumgrana" *)

  let close lwt_dbh =
    lwt dbh = lwt_dbh in
    PGOCaml.close dbh

  let prepare lwt_dbh name query =
    lwt dbh = lwt_dbh in
    print_endline ("\nPrepare:: " ^ name);
    print_endline query;
    PGOCaml.prepare dbh ~query ~name ()

  let prepare_list dbh list =
    let aux (name, query_gen) = prepare dbh name (query_gen ()) in
    lwt () = Utils.Lwt_list.iter_s_exc aux list in
    Lwt.return ()

  let execute lwt_dbh name params =
    lwt dbh = lwt_dbh in
    print_endline ("\nExecute:: " ^ name);
    List.iter (fun x -> match x with | Some x -> print_endline x | _ -> ()) params;
    PGOCaml.execute dbh ~name ~params ()

end

module Row =
struct

  module Util =
  struct

    let print i row =
      Printf.printf "row %d: [%s]\n" i
        (String.concat "; "
           (List.map (function
           | None -> "NULL"
           | Some str -> Printf.sprintf "%S" str) row))

    let prints rows =
      List.iteri print rows

    let opt_get = function
      | Some v -> v
      | None -> raise Not_found

    let get row name pos =
      try opt_get (List.nth row pos)
      with Not_found -> raise (Invalid_argument (name^": Is NULL"))

    let check_size row size type_name =
      if List.length row != size
      then raise (Invalid_argument ("This row is not a "^type_name^" type"))

  end

  let to_content_uri row =
    (* Util.print 0 row; *)
    let () = Util.check_size row 1 "content_uri" in
    Ptype.uri_of_string (Util.get row "content.uri" 0)

  let to_content row =
    (* Util.print 0 row; *)
    let () = Util.check_size row 3 "content" in
    let uri = Ptype.uri_of_string (Util.get row "content.uri" 0) in
    let title = Util.get row "content.title" 1 in
    let summary = Util.get row "content.summary" 2 in
    uri, title, summary

  let to_link_id row =
    (* Util.print 0 row; *)
    let () = Util.check_size row 1 "link_id" in
    int_of_string (Util.get row "link.id" 0)

  let to_link row =
    (* Util.print 0 row; *)
    let () = Util.check_size row 6 "link" in
    let id = int_of_string (Util.get row "link.id" 0) in
    let o_uri = Ptype.uri_of_string (Util.get row "link.origin_uri" 1) in
    let t_uri = Ptype.uri_of_string (Util.get row "link.target_uri" 2) in
    let nature = Util.get row "link.nature" 3 in
    let mark = float_of_string (Util.get row "link.mark" 4) in
    let user_mark = float_of_string (Util.get row "link.user_mark" 6) in
    id, o_uri, t_uri, nature, mark, user_mark

  let to_tag_id row =
    (* Util.print 0 row; *)
    let () = Util.check_size row 1 "tag_id" in
    int_of_string (Util.get row "tag.id" 0)

  let to_tag row =
    (* Util.print 0 row; *)
    let () = Util.check_size row 4 "tag" in
    let id = int_of_string (Util.get row "tag.id" 0) in
    let uri = Ptype.uri_of_string (Util.get row "tag.content_uri" 1) in
    let subject = Util.get row "tag.subject" 2 in
    let mark = float_of_string (Util.get row "tag.mark" 3) in
    id, uri, subject, mark

end

module Query =
struct

  type where =
  | Depend of string
  | Value
  | Values
  | Regexp

  type params =
  | String of string
  | Strings of string list
  | Words of string list
  | Uri of Ptype.uri
  | Uris of Ptype.uri list
  | Float of float
  | Int of int
  | Id of int
  | Ids of int list

  type order =
  | ASC of string
  | DESC of string

  type operator = Nop | And | Or

  type condition = (operator * string * where) list

  module Util =
  struct

    let string_of_operator = function
      | Nop  -> ""
      | And   -> " AND"
      | Or    -> " OR"

    let string_of_format format =
      String.concat ", " format

    let param_generator values =
      let to_string = Ptype.string_of_uri in
      let to_regexp strict strs =
        let strs' = if strict then strs else List.map String.lowercase strs in
        let regexp = "(" ^ (String.concat "|" strs') ^ ")" in
        if strict then regexp else "%"^regexp^"%"
      in
      let to_param = function
        | String str   -> Some str
        | Strings strs -> Some (to_regexp true strs)
        | Words wds    -> Some (to_regexp false wds)
        | Uri uri      -> Some (to_string uri)
        | Uris uris    -> Some (to_regexp true (List.map to_string uris))
        | Float f      -> Some (string_of_float f)
        | Int i        -> Some (string_of_int i)
        | Id id        -> Some (string_of_int id)
        | Ids ids      -> Some (to_regexp true (List.map string_of_int ids))
      in
      List.map to_param values

    let dollar_generator start length =
      let stop = start + length in
      let rec aux dollars current =
        if current < stop
        then aux (("$" ^ (string_of_int current))::dollars) (current + 1)
        else List.rev dollars
      in
      let dollars = aux [] start in
      let str = String.concat ", " dollars in
      "(" ^ str ^ ")"

    let where_generator value_num conditions =
      let aux where name op value_num =
        let dollar = dollar_generator value_num 1 in
        where ^^ name ^^ op ^^ dollar, value_num + 1
      in
      let lower str = "LOWER("^str^")" in
      let as_text str = "CAST("^str^" AS text)" in
      let aux (where, value_num) (operator, name, value_type) =
        let length = String.length where in
        if length = 0 && operator != Nop
        then raise (Invalid_argument "The first operator should be None");
        if length > 0 && operator = Nop
        then raise (Invalid_argument "Only the first operator should be None");
        let sep = string_of_operator operator in
        let where = where ^ sep in
        match value_type with
        | Depend dep -> where ^^ name ^^ "=" ^^ dep, value_num
        | Value      -> aux where name "IN" value_num
        | Values     -> aux where (as_text name) "SIMILAR TO" value_num
        | Regexp     -> aux where (lower name) "SIMILAR TO" value_num
      in
      let where, _ = List.fold_left aux ("", value_num + 1) conditions in
      if String.length where = 0 then "" else "WHERE" ^ where

    let distinct_generator = function
      | None -> ""
      | Some keys ->
        if List.length keys == 0 then ""
        else "DISTINCT ON ("^(String.concat ", " keys)^")"

    let group_generator = function
      | None -> ""
      | Some keys ->
        if List.length keys == 0 then ""
        else "GROUP BY" ^^ (String.concat ", " keys)

    let string_of_order = function
      | ASC key  -> key ^^ "ASC"
      | DESC key -> key ^^ "DESC"

    let order_generator = function
      | None -> ""
      | Some keys ->
        if List.length keys == 0 then ""
        else "ORDER BY" ^^ (String.concat ", " (List.map string_of_order keys))

  end

  let select ?distinct_keys format tables conditions ?group_keys ?order_keys
      max_size =
    if List.length tables < 1
    then raise (Invalid_argument "from table could not be null");
    let distinct = Util.distinct_generator distinct_keys in
    let select = "SELECT" ^^ distinct ^^ (Util.string_of_format format) in
    let from = "FROM" ^^ (String.concat ", " tables) in
    let where = Util.where_generator 0 conditions in
    let group_by = Util.group_generator group_keys in
    let order_by = Util.order_generator order_keys in
    let limit = "LIMIT" ^^ (string_of_int max_size) in
    select ^^ from ^^ where ^^ group_by ^^ order_by ^^ limit

  let insert format table returns =
    let str_format = Util.string_of_format format in
    let insert = "INSERT INTO"^^table^^"("^str_format^")" in
    let values = "VALUES" ^^ (Util.dollar_generator 1 (List.length format)) in
    let returning = "RETURNING" ^^ (String.concat ", " returns) in
    insert ^^ values ^^ returning

  let update format table conditions returns =
    let update = "UPDATE" ^^ table in
    let format_length = List.length format in
    let str_format = Util.string_of_format format in
    let dollars = (Util.dollar_generator 1 format_length) in
    let values = " SET (" ^  str_format ^ ") =" ^^ dollars in
    let where = Util.where_generator format_length conditions in
    let returning = "RETURNING" ^^ (String.concat ", " returns) in
    update ^^ values ^^ where ^^ returning

  let delete table conditions returns =
    let delete = "DELETE FROM" ^^ table in
    let where = Util.where_generator 0 conditions in
    let returning = "RETURNING" ^^ (String.concat ", " returns) in
    delete ^^ where ^^ returning

  let union query1 query2 max_size =
    let union = "(" ^ query1 ^") UNION (" ^ query2 ^ ")" in
    let limit = "LIMIT" ^^ (string_of_int max_size) in
    union ^^ limit

end

(******************************************************************************
*********************************** Content ***********************************
*******************************************************************************)

module Content =
struct

  type t = (Ptype.uri * string * string)

  let to_string (uri, title, summary) =
    "(" ^ (String.concat ", " [Ptype.string_of_uri uri; title; summary]) ^ ")"

  let print content =
    print_endline (to_string content)

  let compare (uri_1, title_1, summary_1) (uri_2, title_2, summary_2) =
    Ptype.compare_uri uri_1 uri_2 +
    String.compare title_1 title_2 +
    String.compare summary_1 summary_2

  module QueryName =
  struct
    let get = "Content.Get"
    let list = "Content.List"
    let list_by_subject = "Content.List-By-Subject"
    let search_by_title_and_summary = "Content.Search-By-Title-And-Summary"
    let search_by_subject = "Content.Search-By-Subject"
    let search = "Content.Search"
    let insert = "Content.Insert"
    let update = "Content.Update"
    let delete = "Content.Delete"
  end

  module QueryGen =
  struct

    let get () =
      let contitions = Query.([(Nop, "content_uri", Value)]) in
      let group_keys = ["content_uri"] in
      Query.select Format.To_get.content [Table.content] contitions
        ~group_keys Conf.internal_limit

    let list () =
      let group_keys = ["content_uri"] in
      Query.select Format.To_get.content [Table.content] []
        ~group_keys Conf.internal_limit

    let list_by_subject () =
      (* "SELECT content.content_uri, title, summary FROM content, tag WHERE content.content_uri = tag.content_uri AND CAST(subject AS text) SIMILAR TO ($1) ORDER BY mark DESC LIMIT 20" *)
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "tag.content_uri");
                (And, "subject", Values)])
      in
      let group_keys = ["content.content_uri"] in
      let order_keys = Query.([DESC "max(mark)"]) in
      Query.select Format.To_get.content Table.([content; tag]) contitions
        ~group_keys ~order_keys Conf.internal_limit

    let search_by_title_and_summary () =
      let contitions = Query.([(Nop, "title", Regexp); (Or, "summary", Regexp)]) in
      let group_keys = ["content_uri"] in
      Query.select Format.To_get.content Table.([content]) contitions
        ~group_keys Conf.internal_limit

    let search_by_subject () =
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "tag.content_uri");
                (And, "subject", Regexp)])
      in
      let group_keys = ["content.content_uri"] in
      let order_keys = Query.([DESC "max(mark)"]) in
      Query.select Format.To_get.content Table.([content; tag]) contitions
        ~group_keys ~order_keys Conf.internal_limit

    let search () =
      let query1 = search_by_title_and_summary () in
      let query2 = search_by_subject () in
      Query.union query1 query2 Conf.internal_limit

    let insert () =
      let returns = ["content_uri"] in
      Query.insert Format.To_set.content Table.content returns

    let update () =
      let contitions = Query.([(Nop, "content_uri", Value)]) in
      let returns = ["content_uri"] in
      Query.update Format.To_set.content Table.content contitions returns

    let delete () =
      let contitions = Query.([(Nop, "content_uri", Values)]) in
      let returns = ["content_uri"] in
      Query.delete Table.content contitions returns

  end

  module Prepare =
  struct

    let list =
      [QueryName.get,                         QueryGen.get;
       QueryName.list,                        QueryGen.list;
       QueryName.list_by_subject,             QueryGen.list_by_subject;
       QueryName.search_by_title_and_summary, QueryGen.search_by_title_and_summary;
       QueryName.search_by_subject,           QueryGen.search_by_subject;
       QueryName.search,                      QueryGen.search;
       QueryName.insert,                      QueryGen.insert;
       QueryName.update,                      QueryGen.update;
       QueryName.delete,                      QueryGen.delete]

    let all dbh = Pg.prepare_list dbh list

  end

  let get dbh content_uri =
    let params = Query.Util.param_generator Query.([Uri content_uri]) in
    lwt results = Pg.execute dbh QueryName.get params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content (List.hd results))

  let list dbh =
    lwt results = Pg.execute dbh QueryName.list [] in
    Lwt.return (List.map Row.to_content results)

  let list_by_subject dbh subjects =
    let params = Query.Util.param_generator Query.([Strings subjects]) in
    lwt results = Pg.execute dbh QueryName.list_by_subject params in
    Lwt.return (List.map Row.to_content results)

  let search_by_title_and_summary dbh fields =
    let params = Query.Util.param_generator Query.([Words fields; Words fields]) in
    lwt results = Pg.execute dbh QueryName.search_by_title_and_summary params in
    Lwt.return (List.map Row.to_content results)

  let search_by_subject dbh fields =
    let params = Query.Util.param_generator Query.([Words fields]) in
    lwt results = Pg.execute dbh QueryName.search_by_subject params in
    Lwt.return (List.map Row.to_content results)

  let search dbh fields =
    let params = Query.Util.param_generator Query.([Words fields; Words fields]) in
    lwt results = Pg.execute dbh QueryName.search params in
    Lwt.return (List.map Row.to_content results)

  let insert dbh (content_uri, title, summary) =
    let params = Query.Util.param_generator
      Query.([Uri content_uri; String title; String summary])
    in
    lwt results = Pg.execute dbh QueryName.insert params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content_uri (List.hd results))

  let update dbh (content_uri, title, summary) =
    let params = Query.Util.param_generator
      Query.([Uri content_uri; String title; String summary; Uri content_uri])
    in
    lwt results = Pg.execute dbh QueryName.update params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content_uri (List.hd results))

  let delete dbh content_uris =
    let params = Query.Util.param_generator Query.([Uris content_uris]) in
    lwt results = Pg.execute dbh QueryName.delete params in
    Lwt.return (List.map Row.to_content_uri results)

end

(******************************************************************************
************************************* Tag *************************************
*******************************************************************************)

module Tag =
struct

  type t = (int * Ptype.uri * string * float)

  let to_string (id, content_uri, subject, mark) =
    "("^(string_of_int id)^", "^(Ptype.string_of_uri content_uri)^", "^
      subject^", "^(string_of_float mark)^")"

  let print tag =
    print_endline (to_string tag)

  let compare (id_1, uri_1, subject_1, mark_1) (id_2, uri_2, subject_2, mark_2) =
    id_1 - id_2 +
    Ptype.compare_uri uri_1 uri_2 +
    String.compare subject_1 subject_2 +
    int_of_float ((mark_1 -. mark_2) *. Conf.mark_precision)

  module QueryName =
  struct
    let get = "Tag.Get"
    let list = "Tag.List"
    let list_by_content_uri = "Tag.List-By-Content-Uri"
    let insert = "Tag.Insert"
    let update = "Tag.Update"
    let delete = "Tag.Delete"
  end

  module QueryGen =
  struct

    let get () =
      let contitions = Query.([(Nop, "tag_id", Value)]) in
      let group_keys = ["tag_id"] in
      Query.select Format.To_get.tag [Table.tag] contitions
        ~group_keys Conf.internal_limit

    let list () =
      let group_keys = ["tag_id"] in
      let order_keys = Query.([DESC "mark"]) in
      Query.select Format.To_get.tag [Table.tag] []
        ~group_keys ~order_keys Conf.internal_limit

    let list_by_content_uri () =
      let contitions = Query.([(Nop, "content_uri", Values)]) in
      let group_keys = ["tag_id"] in
      let order_keys = Query.([DESC "mark"]) in
      Query.select Format.To_get.tag Table.([tag]) contitions
        ~group_keys ~order_keys Conf.internal_limit

    let insert () =
      let returns = ["tag_id"] in
      Query.insert Format.To_set.tag Table.tag returns

    let update () =
      let contitions = Query.([(Nop, "tag_id", Value)]) in
      let returns = ["tag_id"] in
      Query.update Format.To_set.tag Table.tag contitions returns

    let delete () =
      let contitions = Query.([(Nop, "tag_id", Values)]) in
      let returns = ["tag_id"] in
      Query.delete Table.tag contitions returns

  end

  module Prepare =
  struct

    let list =
      [QueryName.get,                         QueryGen.get;
       QueryName.list,                        QueryGen.list;
       QueryName.list_by_content_uri,         QueryGen.list_by_content_uri;
       QueryName.insert,                      QueryGen.insert;
       QueryName.update,                      QueryGen.update;
       QueryName.delete,                      QueryGen.delete]

    let all dbh = Pg.prepare_list dbh list

  end

  let get dbh tag_id =
    let params = Query.Util.param_generator Query.([Id tag_id]) in
    lwt results = Pg.execute dbh QueryName.get params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_tag (List.hd results))

  let list dbh =
    lwt results = Pg.execute dbh QueryName.list [] in
    Lwt.return (List.map Row.to_tag results)

  let list_by_content_uri dbh content_uris =
    let params = Query.Util.param_generator Query.([Uris content_uris]) in
    lwt results = Pg.execute dbh QueryName.list_by_content_uri params in
    Lwt.return (List.map Row.to_tag results)

  let insert dbh (id, content_uri, subject, mark) =
    let params = Query.Util.param_generator
      Query.([Id id; Uri content_uri; String subject; Float mark])
    in
    lwt results = Pg.execute dbh QueryName.insert params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_tag_id (List.hd results))

  let update dbh (id, content_uri, subject, mark) =
    let params = Query.Util.param_generator
      Query.([Id id; Uri content_uri; String subject; Float mark; Id id])
    in
    lwt results = Pg.execute dbh QueryName.update params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_tag_id (List.hd results))

  let delete dbh tag_ids =
    let params = Query.Util.param_generator Query.([Ids tag_ids]) in
    lwt results = Pg.execute dbh QueryName.delete params in
    Lwt.return (List.map Row.to_tag_id results)

end

(******************************************************************************
************************************* Link ************************************
*******************************************************************************)

module Link =
struct

  type t = (int * Ptype.uri * Ptype.uri * string * float * float)

  let to_string (id, origin_uri, target_uri, nature, mark, user_mark) =
    "("^(string_of_int id)^", "^
      (Ptype.string_of_uri origin_uri)^", "^
      (Ptype.string_of_uri target_uri)^", "^
      nature^", "^
      (string_of_float mark)^", "^
      (string_of_float user_mark)^")"

  let print link =
    print_endline (to_string link)

  let compare
      (id_1, origin_uri_1, target_uri_1, nature_1, mark_1, user_mark_1)
      (id_2, origin_uri_2, target_uri_2, nature_2, mark_2, user_mark_2) =
    id_1 - id_2 +
    Ptype.compare_uri origin_uri_1 origin_uri_2 +
    Ptype.compare_uri target_uri_1 target_uri_2 +
    String.compare nature_1 nature_2 +
    int_of_float ((mark_1 -. mark_2) *. Conf.mark_precision) +
    int_of_float ((user_mark_1 -. user_mark_2) *. Conf.mark_precision)

  module QueryName =
  struct
    let tmp_try = "Link.Tmp-Try"
    let get = "Link.Get"
    let list = "Link.List"
    let list_by_content_uri = "Link.List-By-Content-Uri"
    let list_by_content_tag = "Link.List-By-Content-Tag"
    let search = "Link.Search"
    let insert = "Link.Insert"
    let update = "Link.Update"
    let delete = "Link.Delete"
  end

  module QueryGen =
  struct

    let tmp_try () =
      "SELECT DISTINCT ON (target_uri) link_id, target_uri, nature, mark, user_mark FROM link ORDER BY target_uri DESC, mark DESC LIMIT 20"

    let get () =
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "link.target_uri");
                (And, "link_id", Value)])
      in
      let tables = Table.([link; content]) in
      Query.select Format.To_get.linked_content tables contitions
        Conf.internal_limit

    let list () =
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "link.target_uri")])
      in
      let distinct_keys = Query.(["content.content_uri"]) in
      let tables = Table.([link; content]) in
      let order_keys = Query.([DESC "content.content_uri"; DESC "link.mark"]) in
      Query.select ~distinct_keys Format.To_get.linked_content tables contitions
        ~order_keys Conf.internal_limit

    let list_by_content_uri () =
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "link.target_uri");
                (And, "origin_uri", Value)])
      in
      let distinct_keys = Query.(["content.content_uri"]) in
      let tables = Table.([link; content]) in
      let order_keys = Query.([DESC "content.content_uri"; DESC "link.mark"]) in
      Query.select ~distinct_keys Format.To_get.linked_content tables contitions
        ~order_keys Conf.internal_limit

    let list_by_content_tag () =
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "link.target_uri");
                (And, "tag.content_uri", Depend "link.target_uri");
                (And, "origin_uri", Value);
                (And, "subject", Values)])
      in
      let distinct_keys = Query.(["content.content_uri"]) in
      let order_keys = Query.([DESC "content.content_uri"; DESC "link.mark"]) in
      let tables = Table.([link; content; tag]) in
      Query.select ~distinct_keys Format.To_get.linked_content tables contitions
        ~order_keys Conf.internal_limit

    let search () =
      let contitions =
        Query.([(Nop, "tag.content_uri", Depend "link.target_uri");
                (And, "content.content_uri", Depend "link.target_uri");
                (And, "origin_uri", Value);
                (And, "subject", Regexp);
                (Or, "title", Regexp);
                (Or, "summary", Regexp)])
      in
      let distinct_keys = Query.(["content.content_uri"]) in
      let order_keys = Query.([DESC "content.content_uri"; DESC "link.mark"]) in
      let tables = Table.([link; content; tag]) in
      Query.select ~distinct_keys Format.To_get.linked_content tables contitions
        ~order_keys Conf.internal_limit

    let insert () =
      let returns = ["link_id"] in
      Query.insert Format.To_set.link Table.link returns

    let update () =
      let contitions = Query.([(Nop, "link_id", Value)]) in
      let returns = ["link_id"] in
      Query.update Format.To_set.link Table.link contitions returns

    let delete () =
      let contitions = Query.([(Nop, "link_id", Values)]) in
      let returns = ["link_id"] in
      Query.delete Table.link contitions returns

  end

  module Prepare =
  struct

    let list =
      [QueryName.tmp_try,                     QueryGen.tmp_try;
       QueryName.get,                         QueryGen.get;
       QueryName.list,                        QueryGen.list;
       QueryName.list_by_content_uri,         QueryGen.list_by_content_uri;
       QueryName.list_by_content_tag,         QueryGen.list_by_content_tag;
       QueryName.search,                      QueryGen.search;
       QueryName.insert,                      QueryGen.insert;
       QueryName.update,                      QueryGen.update;
       QueryName.delete,                      QueryGen.delete]

    let all dbh = Pg.prepare_list dbh list

  end

  let get dbh link_id =
    let params = Query.Util.param_generator Query.([Id link_id]) in
    lwt results = Pg.execute dbh QueryName.get params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_link (List.hd results))

  let list dbh =
    lwt results = Pg.execute dbh QueryName.list [] in
    Lwt.return (List.map Row.to_link results)

  let list_by_content_uri dbh content_uri =
    let params = Query.Util.param_generator Query.([Uri content_uri]) in
    lwt results = Pg.execute dbh QueryName.list_by_content_uri params in
    Lwt.return (List.map Row.to_link results)

  let list_by_content_tag dbh content_uri subjects =
    let params = Query.Util.param_generator Query.([Uri content_uri;
                                                    Strings subjects])
    in
    lwt results = Pg.execute dbh QueryName.list_by_content_tag params in
    Lwt.return (List.map Row.to_link results)

  let search dbh content_uri research =
    let params = Query.Util.param_generator Query.([Uri content_uri;
                                                    Words research;
                                                    Words research;
                                                    Words research])
    in
    lwt results = Pg.execute dbh QueryName.search params in
    Lwt.return (List.map Row.to_link results)

  let insert dbh (id, origin_uri, target_uri, nature, mark, user_mark) =
    let params = Query.Util.param_generator
      Query.([Id id; Uri origin_uri; Uri target_uri; String nature;
              Float mark; Float user_mark])
    in
    lwt results = Pg.execute dbh QueryName.insert params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_link_id (List.hd results))

  let update dbh (id, origin_uri, target_uri, nature, mark, user_mark) =
    let params = Query.Util.param_generator
      Query.([Id id; Uri origin_uri; Uri target_uri; String nature;
              Float mark; Float user_mark; Id id])
    in
    lwt results = Pg.execute dbh QueryName.update params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_link_id (List.hd results))

  let delete dbh link_ids =
    let params = Query.Util.param_generator Query.([Ids link_ids]) in
    lwt results = Pg.execute dbh QueryName.delete params in
    Lwt.return (List.map Row.to_link_id results)

end

(******************************************************************************
********************************** Functions **********************************
*******************************************************************************)

let module_prepare = [Content.Prepare.all; Tag.Prepare.all; Link.Prepare.all]

let connect () =
  let dbh = Pg.connect () in
  let prepare prepare_fun = prepare_fun dbh in
  lwt () = Utils.Lwt_list.iter_s_exc prepare module_prepare in
  dbh

(******************************************************************************
******************************** Unitary tests ********************************
*******************************************************************************)

let main () =
  let dbh = connect () in

  let to_uri = Ptype.uri_of_string in

  let succeed name =
    print_endline ("Succeed   ::\t"^name);
    Lwt.return ()
  in

  let string_of_content_diff input output =
    ("(input) " ^ Content.to_string input ^ " != " ^
        "(output) " ^ Content.to_string output ^ "\n")
  in

  let string_of_uri_diff input output =
    ("(input) " ^ Ptype.string_of_uri input ^ " != " ^
        "(output) " ^ Ptype.string_of_uri output ^ "\n")
  in

  let uri = to_uri "http://patate.com" in
  let content_1 = (uri, "patateee", "awesome") in
  let content_2 = (uri, "aubergine", "carotte") in

  let failed name desc =
    print_endline ("Failed    ::\t"^name);
    print_endline "Caused By ::";
    print_endline desc;
    lwt _ = Content.delete dbh [uri] in
    exit 0
  in

  let wrap_try name func =
    try_lwt
      lwt _ = func name in
      Lwt.return ()
    with e -> failed name (Printexc.to_string e)
  in

  print_endline "\n### Unitary tests of the PosgreSQL Module ###";

  lwt _ =
    try_lwt Content.delete dbh [uri]
    with _ -> Lwt.return [uri]
  in

  lwt () = wrap_try "Content.Insert" (fun name ->
    lwt uri' = Content.insert dbh content_1 in
    lwt content' = Content.get dbh uri' in
    if Content.compare content_1 content' != 0
    then failed name (string_of_content_diff content_1 content')
    else succeed name)
  in

  lwt () = wrap_try "Content.Update" (fun name ->
    lwt uri' = Content.update dbh content_2 in
    lwt content' = Content.get dbh uri' in
    if Content.compare content_2 content' != 0
    then failed name (string_of_content_diff content_2 content')
    else succeed name)
  in

  lwt () = wrap_try "Content.Get" (fun name ->
    lwt content' = Content.get dbh uri in
    if Content.compare content_2 content' != 0
    then failed name (string_of_content_diff content_2 content')
    else succeed name)
  in

  lwt () = wrap_try "Content.List" (fun name ->
    lwt contents = Content.list dbh in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Content.Search_by_title_and_summary" (fun name ->
    lwt contents = Content.search_by_title_and_summary dbh ["ott"] in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Content.Search" (fun name ->
    lwt contents = Content.search dbh ["aub"] in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

 (*  lwt () = wrap_try "Content.Delete" (fun name -> *)
 (*    lwt uri' = Utils.Lwt_list.hd (Content.delete dbh [uri]) in *)
 (*    lwt _ = *)
 (*      try_lwt Content.get dbh uri *)
 (*      with *)
 (*       | Not_found -> Lwt.return content_2 *)
 (*       | _ -> failed name (Ptype.string_of_uri uri ^ ":: Is not deleted") *)
 (*    in *)
 (*    if Ptype.compare_uri uri uri' != 0 *)
 (*    then failed name (string_of_uri_diff uri uri') *)
 (*    else succeed name) *)
 (* in *)

  let origin_id = 1 in
  let uri = to_uri "http://patate.com" in
  let tag_1 = (origin_id, uri, "Obama", 3.5) in
  let subject = "Barack" in
  let tag_2 = (origin_id, uri, subject, 12.2) in

  let add_id id (id', uri, subject, mark) = (id, uri, subject, mark) in

  let id = ref None in

  let set_id new_id = id := Some new_id in

  let get_id () = match !id with
    | None   -> raise Not_found
    | Some x -> x
  in

  let failed name desc =
    print_endline ("Failed    ::\t"^name);
    print_endline "Caused By ::";
    print_endline desc;
    lwt _ = Tag.delete dbh [get_id ()] in
    lwt _ = Content.delete dbh [uri] in
    exit 0
  in

  let wrap_try name func =
    try_lwt
      lwt _ = func name in
      Lwt.return ()
    with e -> failed name (Printexc.to_string e)
  in

  let string_of_tag_diff input output =
    ("(input) " ^ Tag.to_string input ^ " != " ^
        "(output) " ^ Tag.to_string output ^ "\n")
  in

  let string_of_id_diff input output =
    ("(input) " ^ string_of_int input ^ " != " ^
        "(output) " ^ string_of_int output ^ "\n")
  in

  (* let () = set_id origin_id in *)
  lwt () = wrap_try "Tag.Insert" (fun name ->
    lwt id' = Tag.insert dbh tag_1 in
    let () = set_id id' in
    let full_tag_1 = add_id id' tag_1 in
    lwt tag' = Tag.get dbh id' in
    if Tag.compare full_tag_1 tag' != 0
    then failed name (string_of_tag_diff full_tag_1 tag')
    else succeed name)
  in

  let id = get_id () in
  let full_tag_2 = add_id id tag_2 in

  lwt () = wrap_try "Tag.Update" (fun name ->
    lwt id' = Tag.update dbh tag_2 in
    lwt tag' = Tag.get dbh id' in
    if Tag.compare full_tag_2 tag' != 0
    then failed name (string_of_tag_diff full_tag_2 tag')
    else succeed name)
  in

  lwt () = wrap_try "Tag.Get" (fun name ->
    lwt tag' = Tag.get dbh id in
    if Tag.compare full_tag_2 tag' != 0
    then failed name (string_of_tag_diff full_tag_2 tag')
    else succeed name)
  in

  lwt () = wrap_try "Tag.List" (fun name ->
    lwt tags = Tag.list dbh in
    if List.length tags == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Tag.List_by_content_uri" (fun name ->
    lwt tags = Tag.list_by_content_uri dbh [uri] in
    if List.length tags == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Content.List_by_subject" (fun name ->
    lwt contents = Content.list_by_subject dbh [subject] in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Content.Search_by_subject" (fun name ->
    lwt contents = Content.search_by_subject dbh ["bar"] in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Tag.Delete" (fun name ->
    lwt id' = Utils.Lwt_list.hd (Tag.delete dbh [id]) in
    lwt _ =
      try_lwt Tag.get dbh id'
      with
       | Not_found -> Lwt.return full_tag_2
       | _ -> failed name (string_of_int id ^ ":: Is not deleted")
    in
    if id != id'
    then failed name (string_of_id_diff id id')
    else succeed name)
  in

  lwt () = wrap_try "Content.Delete" (fun name ->
    lwt uri' = Utils.Lwt_list.hd (Content.delete dbh [uri]) in
    lwt _ =
      try_lwt Content.get dbh uri
      with
       | Not_found -> Lwt.return content_2
       | _ -> failed name (Ptype.string_of_uri uri ^ ":: Is not deleted")
    in
    if Ptype.compare_uri uri uri' != 0
    then failed name (string_of_uri_diff uri uri')
    else succeed name)
 in

 print_endline "Done";
 Pg.close dbh

lwt () = main ()
