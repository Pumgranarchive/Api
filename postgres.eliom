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
  let mark_decimal = 3.
  let mark_precision = Utils.Maths.power 10. mark_decimal
end

module Format =
struct

  module To_get =
  struct
    let content = ["content.content_uri"; "title"; "summary"; "content.user_mark"]
    let link = ["link_id"; "origin_uri"; "target_uri";
                "nature"; "link.mark"; "user_mark"]
    let linked_content = ["link_id"; "nature"; "link.mark"; "link.user_mark";
                          "content.content_uri"; "title"; "summary"]
    let tag = ["tag_id"; "tag.content_uri"; "subject"; "tag.mark"]
  end

  module To_set =
  struct
    let content = ["content_uri"; "title"; "summary"; "user_mark"]
    let link = ["link_id"; "origin_uri"; "target_uri"; "nature"; "mark"; "user_mark"]
    let tag = ["tag_id"; "content_uri"; "subject"; "mark"]
  end

end

(******************************************************************************
************************************ Utils ************************************
*******************************************************************************)

let (^^) a b = a ^ " " ^ b

module Pg =
struct

  let connect () = Lwt_PGOCaml.connect
    (* ~host:"127.0.0.1" *)
    ~user:"nox"
    (* ~password:"1234" *)
    ~database:"nox"
    ()

  let close dbh =
    PGOCaml.close dbh

  let prepare dbh name query =
    (* print_endline ("\nPrepare:: " ^ name); *)
    (* print_endline query; *)
    PGOCaml.prepare dbh ~query ~name ()

  let prepare_list dbh list =
    let aux (name, query_gen) = prepare dbh name (query_gen ()) in
    lwt () = Utils.Lwt_list.iter_s_exc aux list in
    Lwt.return ()

  let execute dbh name params =
    (* print_endline ("\nExecute:: " ^ name); *)
    (* List.iter (fun x -> match x with | Some x -> print_endline x | _ -> ()) params; *)
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
    let () = Util.check_size row 4 "content" in
    let uri = Ptype.uri_of_string (Util.get row "content.uri" 0) in
    let title = Util.get row "content.title" 1 in
    let summary = Util.get row "content.summary" 2 in
    let user_mark = float_of_string (Util.get row "content.user_mark" 3) in
    uri, title, summary, user_mark

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
    let user_mark = float_of_string (Util.get row "link.user_mark" 5) in
    id, o_uri, t_uri, nature, mark, user_mark

  let to_linked_content row =
    (* Util.print 0 row; *)
    let () = Util.check_size row 7 "linked_content" in
    let id = int_of_string (Util.get row "link.id" 0) in
    let nature = Util.get row "link.nature" 1 in
    let mark = float_of_string (Util.get row "link.mark" 2) in
    let user_mark = float_of_string (Util.get row "link.user_mark" 3) in
    let uri = Ptype.uri_of_string (Util.get row "content.uri" 4) in
    let title = Util.get row "content.title" 5 in
    let summary = Util.get row "content.summary" 6 in
    id, nature, mark, user_mark, uri, title, summary

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

    let dollar_generator ?(first_default=false) start length =
      let length = if first_default then length -1 else length in
      let stop = start + length in
      let rec aux dollars current =
        if current < stop
        then aux (("$" ^ (string_of_int current))::dollars) (current + 1)
        else List.rev dollars
      in
      let dollars = aux [] start in
      let dollars = if first_default then "DEFAULT"::dollars else dollars in
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

  let insert ?first_default format table returns =
    let length = List.length format in
    let str_format = Util.string_of_format format in
    let insert = "INSERT INTO"^^table^^"("^str_format^")" in
    let values = "VALUES" ^^ (Util.dollar_generator ?first_default 1 length) in
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

  type t = (Ptype.uri * string * string * float)

  let to_string (uri, title, summary, user_mark) =
    "(" ^ (String.concat ", "
             [Ptype.string_of_uri uri; title; summary;
              string_of_float user_mark]) ^ ")"

  let print content =
    print_endline (to_string content)

  let compare
      (uri_1, title_1, summary_1, user_mark_1)
      (uri_2, title_2, summary_2, user_mark_2) =
    Ptype.compare_uri uri_1 uri_2 +
    String.compare title_1 title_2 +
    String.compare summary_1 summary_2 +
    int_of_float ((user_mark_1 -. user_mark_2) *. Conf.mark_precision)

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
        ~group_keys Conf.limit

    let list () =
      let group_keys = ["content_uri"] in
      Query.select Format.To_get.content [Table.content] []
        ~group_keys Conf.limit

    let list_by_subject () =
      (* "SELECT content.content_uri, title, summary FROM content, tag WHERE content.content_uri = tag.content_uri AND CAST(subject AS text) SIMILAR TO ($1) ORDER BY mark DESC LIMIT 20" *)
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "tag.content_uri");
                (And, "subject", Values)])
      in
      let group_keys = ["content.content_uri"] in
      let order_keys = Query.([DESC "max(mark)"]) in
      Query.select Format.To_get.content Table.([content; tag]) contitions
        ~group_keys ~order_keys Conf.limit

    let search_by_title_and_summary () =
      let contitions = Query.([(Nop, "title", Regexp); (Or, "summary", Regexp)]) in
      let group_keys = ["content_uri"] in
      Query.select Format.To_get.content Table.([content]) contitions
        ~group_keys Conf.limit

    let search_by_subject () =
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "tag.content_uri");
                (And, "subject", Regexp)])
      in
      let group_keys = ["content.content_uri"] in
      let order_keys = Query.([DESC "max(mark)"]) in
      Query.select Format.To_get.content Table.([content; tag]) contitions
        ~group_keys ~order_keys Conf.limit

    let search () =
      let query1 = search_by_title_and_summary () in
      let query2 = search_by_subject () in
      Query.union query1 query2 Conf.limit

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

  let insert dbh (content_uri, title, summary, user_mark) =
    let params = Query.Util.param_generator
      Query.([Uri content_uri; String title; String summary; Float user_mark])
    in
    lwt results = Pg.execute dbh QueryName.insert params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content_uri (List.hd results))

  let update dbh (content_uri, title, summary, user_mark) =
    let params = Query.Util.param_generator
      Query.([Uri content_uri; String title; String summary; Float user_mark;
              Uri content_uri])
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
    let search = "Tag.Search"
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
        ~group_keys Conf.limit

    let list () =
      let group_keys = ["tag_id"] in
      let order_keys = Query.([DESC "mark"]) in
      Query.select Format.To_get.tag [Table.tag] []
        ~group_keys ~order_keys Conf.limit

    let list_by_content_uri () =
      let contitions = Query.([(Nop, "content_uri", Value)]) in
      let group_keys = ["tag_id"] in
      let order_keys = Query.([DESC "mark"]) in
      Query.select Format.To_get.tag Table.([tag]) contitions
        ~group_keys ~order_keys Conf.limit

    let search () =
      let contitions = Query.([(Nop, "subject", Regexp)]) in
      let distinct_keys = ["subject"] in
      let group_keys = ["tag_id"] in
      let order_keys = Query.([DESC "subject"; DESC "max(mark)"]) in
      Query.select Format.To_get.tag Table.([tag]) contitions
        ~distinct_keys ~group_keys ~order_keys Conf.limit

    let insert () =
      let first_default = true in
      let returns = ["tag_id"] in
      Query.insert ~first_default Format.To_set.tag Table.tag returns

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
       QueryName.search,                      QueryGen.search;
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

  let list_by_content_uri dbh content_uri =
    let params = Query.Util.param_generator Query.([Uri content_uri]) in
    lwt results = Pg.execute dbh QueryName.list_by_content_uri params in
    Lwt.return (List.map Row.to_tag results)

  let search dbh fields =
    let params = Query.Util.param_generator Query.([Words fields]) in
    lwt results = Pg.execute dbh QueryName.search params in
    Lwt.return (List.map Row.to_tag results)

  let insert dbh (content_uri, subject, mark) =
    let params = Query.Util.param_generator
      Query.([Uri content_uri; String subject; Float mark])
    in
    lwt results = Pg.execute dbh QueryName.insert params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_tag_id (List.hd results))

  let update dbh id (content_uri, subject, mark) =
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
    let insert = "Link.Insert"
    let update = "Link.Update"
    let delete = "Link.Delete"
  end

  module QueryGen =
  struct

    let insert () =
      let first_default = true in
      let returns = ["link_id"] in
      Query.insert ~first_default Format.To_set.link Table.link returns

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
      [QueryName.insert,                      QueryGen.insert;
       QueryName.update,                      QueryGen.update;
       QueryName.delete,                      QueryGen.delete]

    let all dbh = Pg.prepare_list dbh list

  end

  let insert dbh (origin_uri, target_uri, nature, mark, user_mark) =
    let params = Query.Util.param_generator
      Query.([Uri origin_uri; Uri target_uri; String nature;
              Float mark; Float user_mark])
    in
    lwt results = Pg.execute dbh QueryName.insert params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_link_id (List.hd results))

  let update dbh id (origin_uri, target_uri, nature, mark, user_mark) =
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
******************************** Linked Content *******************************
*******************************************************************************)

module LinkedContent =
struct

  type t = (int * string * float * float * Ptype.uri * string * string)

  let to_string
      (id, nature, mark, user_mark, uri, title, summary) =
    "("^(string_of_int id)^", "^
      nature^", "^
      (string_of_float mark)^", "^
      (string_of_float user_mark)^", "^
      (Ptype.string_of_uri uri)^", "^
      title^", "^
      summary^")"

  let print linked_content =
    print_endline (to_string linked_content)

  let compare
      (id_1, nature_1, mark_1, user_mark_1, uri_1, title_1, summary_1)
      (id_2, nature_2, mark_2, user_mark_2, uri_2, title_2, summary_2) =
    id_1 - id_2 +
    String.compare nature_1 nature_2 +
    int_of_float ((mark_1 -. mark_2) *. Conf.mark_precision) +
    int_of_float ((user_mark_1 -. user_mark_2) *. Conf.mark_precision) +
    Ptype.compare_uri uri_1 uri_2 +
    String.compare title_1 title_2 +
    String.compare summary_1 summary_2

  module QueryName =
  struct
    let get = "LinkedContent.Get"
    let list = "LinkedContent.List"
    let list_by_content_uri = "LinkedContent.List-By-Content-Uri"
    let list_by_content_tag = "LinkedContent.List-By-Content-Tag"
    let search = "LinkedContent.Search"
  end

  module QueryGen =
  struct

    let get () =
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "link.target_uri");
                (And, "link_id", Value)])
      in
      let tables = Table.([link; content]) in
      Query.select Format.To_get.linked_content tables contitions
        Conf.limit

    let list () =
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "link.target_uri")])
      in
      let distinct_keys = Query.(["content.content_uri"]) in
      let tables = Table.([link; content]) in
      let order_keys = Query.([DESC "content.content_uri"; DESC "link.mark"]) in
      Query.select ~distinct_keys Format.To_get.linked_content tables contitions
        ~order_keys Conf.limit

    let list_by_content_uri () =
      let contitions =
        Query.([(Nop, "content.content_uri", Depend "link.target_uri");
                (And, "origin_uri", Value)])
      in
      let distinct_keys = Query.(["content.content_uri"]) in
      let tables = Table.([link; content]) in
      let order_keys = Query.([DESC "content.content_uri"; DESC "link.mark"]) in
      Query.select ~distinct_keys Format.To_get.linked_content tables contitions
        ~order_keys Conf.limit

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
        ~order_keys Conf.limit

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
        ~order_keys Conf.limit

  end

  module Prepare =
  struct

    let list =
      [QueryName.get,                         QueryGen.get;
       QueryName.list,                        QueryGen.list;
       QueryName.list_by_content_uri,         QueryGen.list_by_content_uri;
       QueryName.list_by_content_tag,         QueryGen.list_by_content_tag;
       QueryName.search,                      QueryGen.search]

    let all dbh = Pg.prepare_list dbh list

  end

  let get dbh link_id =
    let params = Query.Util.param_generator Query.([Id link_id]) in
    lwt results = Pg.execute dbh QueryName.get params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_linked_content (List.hd results))

  let list dbh =
    lwt results = Pg.execute dbh QueryName.list [] in
    Lwt.return (List.map Row.to_linked_content results)

  let list_by_content_uri dbh content_uri =
    let params = Query.Util.param_generator Query.([Uri content_uri]) in
    lwt results = Pg.execute dbh QueryName.list_by_content_uri params in
    Lwt.return (List.map Row.to_linked_content results)

  let list_by_content_tag dbh content_uri subjects =
    let params = Query.Util.param_generator Query.([Uri content_uri;
                                                    Strings subjects])
    in
    lwt results = Pg.execute dbh QueryName.list_by_content_tag params in
    Lwt.return (List.map Row.to_linked_content results)

  let search dbh content_uri research =
    let params = Query.Util.param_generator Query.([Uri content_uri;
                                                    Words research;
                                                    Words research;
                                                    Words research])
    in
    lwt results = Pg.execute dbh QueryName.search params in
    Lwt.return (List.map Row.to_linked_content results)

end

(******************************************************************************
********************************** Functions **********************************
*******************************************************************************)

let mfun_prepare = [Content.Prepare.all; Tag.Prepare.all;
                    Link.Prepare.all; LinkedContent.Prepare.all]

let connect () =
  lwt dbh = Pg.connect () in
  let () = Lwt_PGOCaml.set_private_data dbh 42 in
  let prepare prepare_fun = prepare_fun dbh in
  lwt () = Utils.Lwt_list.iter_s_exc prepare mfun_prepare in
  Lwt.return dbh

let close = Pg.close
