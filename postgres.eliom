(*
  PosgreSQL
  This module simplify request to PosgreSQL
*)

(******************************************************************************
******************************* Configuration *********************************
*******************************************************************************)

open Utils
module Conf = Conf.Configuration

let () = Random.self_init ()

module Lwt_thread = struct
  include Lwt
  include Lwt_chan
end
module Lwt_PGOCaml = PGOCaml_generic.Make(Lwt_thread)
module PGOCaml = Lwt_PGOCaml

module Error =
struct

  type t =
  | NotFoundQuery of string
  | Error of (int * string)
  | Unkown

  let space = Str.regexp "[ \t]+"
  let dirty = Str.regexp "[:\\\"]+"

  let read = function
    | PGOCaml.PostgreSQL_Error(error, _) ->
      (match Str.split space (Str.global_replace dirty "" error) with
      | "ERROR" :: "26000" :: _ :: _ :: name :: _ -> NotFoundQuery name
      | "ERROR" :: nb :: _ :: _ :: name :: _ -> Error (int_of_string nb, name)
      | _ -> Unkown)
    | e -> raise e

end

module Table =
struct

  type name =
  | As of string

  type t =
  | Table of string
  | Query of (string * name)

  let content = Table "content"
  let tag = Table "tag"
  let link = Table "link"

end

module Format =
struct

  module Get =
  struct
    let content = ["content.content_uri"; "title"; "summary"; "content.user_mark"]
    let link = ["link_id"; "origin_uri"; "target_uri";
                "nature"; "link.mark"; "user_mark"]
    let linked_content = ["link_id"; "nature"; "link.mark"; "link.user_mark";
                          "content.content_uri"; "title"; "summary"]
    let tag = ["tag_id"; "tag.content_uri"; "subject"; "tag.mark"]
  end

  module Insert =
  struct
    let content = ["content_uri"; "title"; "summary"; "user_mark"]
    let link = ["origin_uri"; "target_uri"; "nature"; "mark"; "user_mark"]
    let tag = ["content_uri"; "subject"; "mark"]
  end

  module Update =
  struct
    let content = ["content_uri"; "title"; "summary"]
    let link = ["origin_uri"; "target_uri"; "nature"; "mark"]
    let tag = ["content_uri"; "subject"; "mark"]
  end

end

(******************************************************************************
**************************** Connection Manager *******************************
*******************************************************************************)

let (>>=) = Lwt.bind

module Pg =
struct

  let connect () = Lwt_PGOCaml.connect
    ?host:Conf.Postgres.host
    ~user:Conf.Postgres.user
    ?password:Conf.Postgres.pwd
    ~database:Conf.Postgres.db
    ()

  let close dbh =
    PGOCaml.close dbh

  let prepare dbh name query =
    if Conf.Api.verbose then Printf.printf "\nPrepare :: %s\n%s\n" name query;
    PGOCaml.prepare dbh ~query ~name ()

  let prepare_list dbh list =
    let aux (name, query_gen) = prepare dbh name (query_gen ()) in
    lwt () = Utils.Lwt_list.iter_s_exc aux list in
    Lwt.return ()

  let execute dbh name params =
    if Conf.Api.verbose then
      begin
        Printf.printf "\nExecute :: %s\n" name;
        List.iter (fun x -> match x with | Some x -> print_endline x | _ -> ()) params
      end;
    PGOCaml.execute dbh ~name ~params ()

  let (^^) a b = a ^ "_" ^ b

  let runtime_execute dbh name query params =
    if Conf.Api.verbose then
      begin
        print_endline ("\nRuntime Execute:: " ^ name);
        List.iter (fun x -> match x with | Some x -> print_endline x | _ -> ()) params
      end;
    let run () = PGOCaml.execute dbh ~name ~params () in
    let prepare () = PGOCaml.prepare dbh ~query ~name () in
    try_lwt run ()
    with e -> match Error.read e with
    | Error.NotFoundQuery name -> prepare () >>= run
    | e -> begin print_endline ("PGOcaml error ::\n" ^ query); Lwt.return [] end

end

(******************************************************************************
********************************* Row Manager *********************************
*******************************************************************************)

let (^^) a b = a ^ " " ^ b

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

(******************************************************************************
******************************* Queries Manager *******************************
*******************************************************************************)

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

  type operator2 =
  | And of ((string * where) * operator2)
  | Or of ((string * where) * operator2)
  | End

  type operator =
  | First of ((string * where) * operator2)
  | All

  module Util =
  struct

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

    let where_generator dollar_num conditions =

      let rec close_bracket n =
        if n <= 0 then "" else ")" ^ (close_bracket (n -1))
      in

      let to_string (old_where, outer_op) name inner_op dollar_num =
        let dollar = dollar_generator dollar_num 1 in
        old_where ^^ outer_op ^^ "(" ^ name ^^ inner_op ^^ dollar,
        dollar_num + 1
      in
      let dep_to_string (old_where, outer_op) name dep dollar_num =
        old_where ^^ outer_op ^^ "(" ^ name ^^ "=" ^^ dep, dollar_num
      in

      let lower str = "LOWER("^str^")" in
      let as_text str = "CAST("^str^" AS text)" in
      let builder data name dollar_num = function
        | Depend dep -> dep_to_string data name dep dollar_num
        | Value      -> to_string data name "IN" dollar_num
        | Values     -> to_string data (as_text name) "SIMILAR TO" dollar_num
        | Regexp     -> to_string data (lower name) "SIMILAR TO" dollar_num
      in

      let rec fold (old_where, dollar_num, num) conditions =
        let loop outer_op name value_type next =
          let data = old_where, outer_op in
          let where', dollar_num' = builder data name dollar_num value_type in
          fold (where', dollar_num', num + 1) next
        in
        match conditions with
        | End -> old_where, num
        | And ((name, value_type), next) -> loop "AND" name value_type next
        | Or ((name, value_type), next) -> loop "OR" name value_type next
      in

      let init = ("", "") in
      match conditions with
      | All -> ""
      | First ((name, value_type), next) ->
        let first, dollar_num' = builder init name (dollar_num + 1) value_type in
        let where, brackets = fold (first, dollar_num', 1) next in
        "WHERE" ^ where ^ (close_bracket brackets)

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

    let from_generator tables =
      let builder = function
        | Table.Table table_name -> table_name
        | Table.Query (query, Table.As as_name) -> "("^ query ^") AS "^ as_name
      in
      String.concat ", " (List.map builder tables)

    let string_of_table = function
      | Table.Table table_name -> table_name
      | _ -> raise (Invalid_argument "Only table should be given")

  end

  let select ?distinct_keys format tables conditions ?(first_dollars=0)
      ?group_keys ?order_keys max_size =
    if List.length tables < 1
    then raise (Invalid_argument "from table could not be null");
    let distinct = Util.distinct_generator distinct_keys in
    let select = "SELECT" ^^ distinct ^^ (Util.string_of_format format) in
    let from = "FROM" ^^ (Util.from_generator tables) in
    let where = Util.where_generator first_dollars conditions in
    let group_by = Util.group_generator group_keys in
    let order_by = Util.order_generator order_keys in
    let limit =
      if (max_size <= 0) then "" else ("LIMIT" ^^ (string_of_int max_size))
    in
    select ^^ from ^^ where ^^ group_by ^^ order_by ^^ limit

  let insert ?first_default format
      ?(dollars_offset=1) ?(values_nb=1)
      table returns =
    let length = List.length format in
    let stop = dollars_offset + length * values_nb in
    let rec dollars_manager pos dollars =
      if pos >= stop then dollars else
        let sep = if String.length dollars > 0 then ", " else "" in
        let dollars' = (Util.dollar_generator ?first_default pos length) in
        dollars_manager (pos + length) (dollars ^ sep ^ dollars')
    in
    let str_format = Util.string_of_format format in
    let insert = "INSERT INTO"^^ (Util.string_of_table table) ^^"("^str_format^")" in
    let values = "VALUES" ^^ (dollars_manager dollars_offset "") in
    let returning = "RETURNING" ^^ (String.concat ", " returns) in
    insert ^^ values ^^ returning

  let update format ?(dollars_offset=1) table conditions returns =
    let update = "UPDATE" ^^ (Util.string_of_table table) in
    let format_length = List.length format in
    let str_format = Util.string_of_format format in
    let dollars = (Util.dollar_generator dollars_offset format_length) in
    let values = " SET (" ^  str_format ^ ") =" ^^ dollars in
    let where = Util.where_generator format_length conditions in
    let returning = "RETURNING" ^^ (String.concat ", " returns) in
    update ^^ values ^^ where ^^ returning

  let delete table conditions returns =
    let delete = "DELETE FROM" ^^ (Util.string_of_table table) in
    let where = Util.where_generator 0 conditions in
    let returning = "RETURNING" ^^ (String.concat ", " returns) in
    delete ^^ where ^^ returning

  let union query1 query2 max_size =
    let union = "(" ^ query1 ^") UNION (" ^ query2 ^ ")" in
    let limit = "LIMIT" ^^ (string_of_int max_size) in
    union ^^ limit

  let if_not_found exists query1 query2 =
    "IF EXISTS ("^ exists ^") "^
    "THEN (" ^ query1 ^ ") "^
    "ELSE (" ^ query2 ^ ") END IF;"
    (* query1 ^ "; IF NOT FOUND THEN (" ^ query2 ^ "); END IF;" *)

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
    int_of_float ((user_mark_1 -. user_mark_2) *. Conf.Postgres.mark_precision)

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
    let upsert = "Content.Upsert"
    let delete = "Content.Delete"
  end

  module QueryGen =
  struct

    let get () =
      let contitions = Query.(First (("content_uri", Value), End)) in
      let group_keys = ["content_uri"] in
      Query.select Format.Get.content [Table.content] contitions
        ~group_keys Conf.Postgres.limit

    let list () =
      let group_keys = ["content_uri"] in
      Query.select Format.Get.content [Table.content] Query.All
        ~group_keys Conf.Postgres.limit

    let list_by_subject () =
      let contitions =
        Query.(First (("content.content_uri", Depend "tag.content_uri"),
               And (("subject", Values), End)))
      in
      let group_keys = ["content.content_uri"] in
      let order_keys = Query.([DESC "max(mark)"]) in
      Query.select Format.Get.content Table.([content; tag]) contitions
        ~group_keys ~order_keys Conf.Postgres.limit

    let search_by_title_and_summary () =
      let contitions = Query.(First (("title", Regexp),
                              Or (("summary", Regexp), End)))
      in
      let group_keys = ["content_uri"] in
      Query.select Format.Get.content Table.([content]) contitions
        ~group_keys Conf.Postgres.limit

    let search_by_subject () =
      let contitions =
        Query.(First (("content.content_uri", Depend "tag.content_uri"),
               And (("subject", Regexp), End)))
      in
      let group_keys = ["content.content_uri"] in
      let order_keys = Query.([DESC "max(mark)"]) in
      Query.select Format.Get.content Table.([content; tag]) contitions
        ~group_keys ~order_keys Conf.Postgres.limit

    let search () =
      let contitions =
        Query.(First (("content.content_uri", Depend "tag.content_uri"),
               And (("title", Regexp),
               Or (("summary", Regexp),
               Or (("subject", Regexp), End)))))
      in
      let group_keys = ["content.content_uri"] in
      Query.select Format.Get.content Table.([content; tag]) contitions
        ~group_keys Conf.Postgres.limit

    let base_insert ?dollars_offset () =
      let returns = ["content_uri"] in
      Query.insert Format.Insert.content ?dollars_offset Table.content returns

    let insert () = base_insert ()

    let base_update ?dollars_offset () =
      let contitions = Query.(First (("content_uri", Value), End)) in
      let returns = ["content_uri"] in
      Query.update Format.Update.content ?dollars_offset
        Table.content contitions returns

    let update () = base_update ()

    let upsert () =
      let exists_query = get () in
      let update_query = base_update ~dollars_offset:2 () in
      let insert_query = base_insert ~dollars_offset:6 () in
      Query.if_not_found exists_query update_query insert_query

    let delete () =
      let contitions = Query.(First (("content_uri", Values), End)) in
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
       (* QueryName.upsert,                      QueryGen.upsert; *)
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
    let params =
      Query.Util.param_generator Query.([Words fields; Words fields; Words fields])
    in
    lwt results = Pg.execute dbh QueryName.search params in
    Lwt.return (List.map Row.to_content results)

  let insert dbh (content_uri, title, summary, user_mark) =
    let params = Query.Util.param_generator
      Query.([Uri content_uri; String title; String summary; Float user_mark])
    in
    lwt results = Pg.execute dbh QueryName.insert params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content_uri (List.hd results))

  let update dbh (content_uri, title, summary) =
    let params = Query.Util.param_generator
      Query.([Uri content_uri; String title; String summary;
              Uri content_uri])
    in
    lwt results = Pg.execute dbh QueryName.update params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content_uri (List.hd results))

  let upsert dbh (content_uri, title, summary, user_mark) =
    let params = Query.Util.param_generator
      Query.([Uri content_uri;
              Uri content_uri; String title; String summary; Uri content_uri;
              Uri content_uri; String title; String summary; Float user_mark])
    in
    lwt results = Pg.execute dbh QueryName.upsert params in
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
    int_of_float ((mark_1 -. mark_2) *. Conf.Postgres.mark_precision)

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
      let contitions = Query.(First (("tag_id", Value), End)) in
      let group_keys = ["tag_id"] in
      Query.select Format.Get.tag [Table.tag] contitions
        ~group_keys Conf.Postgres.limit

    let list () =
      let group_keys = ["tag_id"] in
      let order_keys = Query.([DESC "mark"]) in
      Query.select Format.Get.tag [Table.tag] Query.All
        ~group_keys ~order_keys Conf.Postgres.limit

    let list_by_content_uri () =
      let contitions = Query.(First (("content_uri", Value), End)) in
      let group_keys = ["tag_id"] in
      let order_keys = Query.([DESC "mark"]) in
      Query.select Format.Get.tag Table.([tag]) contitions
        ~group_keys ~order_keys Conf.Postgres.limit

    let search () =
      let contitions = Query.(First (("subject", Regexp), End)) in
      let distinct_keys = ["subject"] in
      let group_keys = ["tag_id"] in
      let order_keys = Query.([DESC "subject"; DESC "max(mark)"]) in
      Query.select Format.Get.tag Table.([tag]) contitions
        ~distinct_keys ~group_keys ~order_keys Conf.Postgres.limit

    let runtime_insert ?values_nb () =
      let returns = ["tag_id"] in
      Query.insert Format.Insert.tag ?values_nb Table.tag returns

    let insert () = runtime_insert ()

    let update () =
      let contitions = Query.(First (("tag_id", Value), End)) in
      let returns = ["tag_id"] in
      Query.update Format.Update.tag Table.tag contitions returns

    let delete () =
      let contitions = Query.(First (("tag_id", Values), End)) in
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

  let inserts dbh tags =
    let params_manager tags (content_uri, subject, mark)=
      Query.([Uri content_uri; String subject; Float mark]) @ tags
    in
    let values_nb = List.length tags in
    let name = "tag_insert_" ^ (string_of_int values_nb) in
    let formated_tags = List.fold_left params_manager [] tags in
    let params = Query.Util.param_generator formated_tags in
    let query = QueryGen.runtime_insert ~values_nb () in
    lwt results = Pg.runtime_execute dbh name query params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (List.map Row.to_tag_id results)

  let update dbh id (content_uri, subject, mark) =
    let params = Query.Util.param_generator
      Query.([Uri content_uri; String subject; Float mark; Id id])
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
    int_of_float ((mark_1 -. mark_2) *. Conf.Postgres.mark_precision) +
    int_of_float ((user_mark_1 -. user_mark_2) *. Conf.Postgres.mark_precision)

  module QueryName =
  struct
    let insert = "Link.Insert"
    let update = "Link.Update"
    let delete = "Link.Delete"
  end

  module QueryGen =
  struct

    let runtime_insert ?values_nb () =
      let returns = ["link_id"] in
      Query.insert Format.Insert.link ?values_nb Table.link returns

    let insert () = runtime_insert ()

    let update () =
      let contitions = Query.(First (("link_id", Value), End)) in
      let returns = ["link_id"] in
      Query.update Format.Update.link Table.link contitions returns

    let delete () =
      let contitions = Query.(First (("link_id", Values), End)) in
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

  let inserts dbh links =
    let params_manager list (origin_uri, target_uri, nature, mark, user_mark) =
      Query.([Uri origin_uri; Uri target_uri; String nature;
              Float mark; Float user_mark]) @ list
    in
    let values_nb = List.length links in
    let name = "link_insert_" ^ (string_of_int values_nb) in
    let formated_links = List.fold_left params_manager [] links in
    let params = Query.Util.param_generator formated_links in
    let runtime_query = QueryGen.runtime_insert ~values_nb () in
    lwt results = Pg.runtime_execute dbh name runtime_query params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (List.rev (List.map Row.to_link_id results))

  let update dbh id (origin_uri, target_uri, nature, mark) =
    let params = Query.Util.param_generator
      Query.([Uri origin_uri; Uri target_uri; String nature; Float mark; Id id])
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
    int_of_float ((mark_1 -. mark_2) *. Conf.Postgres.mark_precision) +
    int_of_float ((user_mark_1 -. user_mark_2) *. Conf.Postgres.mark_precision) +
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
        Query.(First (("content.content_uri", Depend "link.target_uri"),
               And (("link_id", Value), End)))
      in
      let tables = Table.([link; content]) in
      Query.select Format.Get.linked_content tables contitions
        Conf.Postgres.limit

    let list () =
      let contitions =
        Query.(First (("content.content_uri", Depend "link.target_uri"), End))
      in
      let distinct_keys = Query.(["content.content_uri"]) in
      let tables = Table.([link; content]) in
      let order_keys = Query.([DESC "content.content_uri"; DESC "link.mark"]) in
      Query.select ~distinct_keys Format.Get.linked_content tables contitions
        ~order_keys Conf.Postgres.limit

    let list_by_content_uri () =
      let contitions =
        Query.(First (("content.content_uri", Depend "link.target_uri"),
               And (("origin_uri", Value), End)))
      in
      let distinct_keys = Query.(["content.content_uri"]) in
      let tables = Table.([link; content]) in
      let order_keys = Query.([DESC "content.content_uri"; DESC "link.mark"]) in
      Query.select ~distinct_keys Format.Get.linked_content tables contitions
        ~order_keys Conf.Postgres.limit

    let list_by_content_tag () =
      let contitions =
        Query.(First (("content.content_uri", Depend "link.target_uri"),
               And (("tag.content_uri", Depend "link.target_uri"),
               And (("origin_uri", Value),
               And (("subject", Values), End)))))
      in
      let distinct_keys = Query.(["content.content_uri"]) in
      let order_keys = Query.([DESC "content.content_uri"; DESC "link.mark"]) in
      let tables = Table.([link; content; tag]) in
      Query.select ~distinct_keys Format.Get.linked_content tables contitions
        ~order_keys Conf.Postgres.limit

    let search () =

      (* SUB QUERY - Select linkedcontent *)
      let where' =
        Query.(First (("content.content_uri", Depend "link.target_uri"),
                      And (("origin_uri", Value), End)))
      in
      let tables' = Table.([content; link]) in
      let order_keys = Query.([DESC "link.mark"]) in
      let query' =
        Query.select Format.Get.linked_content tables' where' ~order_keys 0
      in

      (* SECOND QUERY - filter on search field *)
      let where'' =
        Query.(First (("tag.content_uri", Depend "linkedcontent.content_uri"),
               And (("subject", Regexp),
               Or (("title", Regexp),
               Or (("summary", Regexp), End)))))
      in
      let format = ["link_id"; "nature"; "linkedcontent.mark"; "user_mark";
                    "linkedcontent.content_uri"; "title"; "summary"]
      in
      let group_keys = format in
      let tables'' = Table.([Query (query', As "linkedcontent"); tag]) in
      let first_dollars = 1 in
      Query.select format tables'' where'' ~group_keys ~first_dollars Conf.Postgres.limit

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
