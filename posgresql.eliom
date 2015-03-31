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
end

module Format =
struct

  module To_get =
  struct
    let content = ["content.content_uri"; "title"; "summary"]
    let link = ["link_id"; "origin_uri"; "target_uri";
                "nature"; "link.mark"; "user_mark"]
    let tag = ["tag_id"; "tag.content_uri"; "subject"; "tag.mark"]
  end

  module To_set =
  struct
    let content = ["content_uri"; "title"; "summary"]
    let link = ["origin_uri"; "target_uri"; "nature"; "mark"; "user_mark"]
    let tag = ["content_uri"; "subject"; "tag.mark"]
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
    Util.print 0 row;
    let () = Util.check_size row 1 "content_uri" in
    Ptype.uri_of_string (Util.get row "content.uri" 0)

  let to_content row =
    Util.print 0 row;
    let () = Util.check_size row 3 "content" in
    let uri = Ptype.uri_of_string (Util.get row "content.uri" 0) in
    let title = Util.get row "content.title" 1 in
    let summary = Util.get row "content.summary" 2 in
    uri, title, summary

  let to_link row =
    Util.print 0 row;
    let () = Util.check_size row 6 "link" in
    let id = int_of_string (Util.get row "link.id" 0) in
    let o_uri = Ptype.uri_of_string (Util.get row "link.origin_uri" 1) in
    let t_uri = Ptype.uri_of_string (Util.get row "link.target_uri" 2) in
    let nature = Util.get row "link.nature" 3 in
    let mark = float_of_string (Util.get row "link.mark" 4) in
    let user_mark = float_of_string (Util.get row "link.user_mark" 6) in
    id, o_uri, t_uri, nature, mark, user_mark

  let to_tag row =
    Util.print 0 row;
    let () = Util.check_size row 4 "tag" in
    let id = int_of_string (Util.get row "tag.id" 0) in
    let uri = Ptype.uri_of_string (Util.get row "tag.content_uri" 1) in
    let subject = Util.get row "tag.subject" 2 in
    let mark = float_of_string (Util.get row "tag.mark" 3) in
    id, uri, subject, mark

end

module Query =
struct

  type where_type =
  | Depend of string
  | Value
  | Values

  type params =
  | String of string
  | Strings of string list
  | Words of string list
  | Uri of Ptype.uri
  | Uris of Ptype.uri list

  type operator = None | And | Or

  type condition = (operator * string * where_type) list

  module Util =
  struct

    let string_of_operator = function
      | None  -> ""
      | And   -> " AND "
      | Or    -> " OR "

    let string_of_format format =
      String.concat ", " format

    let param_generator values =
      let to_string = Ptype.string_of_uri in
      let to_regexp modulo strs =
        let regexp = String.concat "" ["("; (String.concat "|" strs); ")"] in
        if modulo then String.concat "" ["%"; regexp; "%"] else regexp
      in
      let to_param = function
        | String str   -> Some str
        | Strings strs -> Some (to_regexp false strs)
        | Words wds    -> Some (to_regexp true wds)
        | Uri uri      -> Some (to_string uri)
        | Uris uris    -> Some (to_regexp false (List.map to_string uris))
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
      let where_value_gen where op value_num =
        let dollar = dollar_generator value_num 1 in
        where ^^ op ^^ dollar, value_num + 1
      in
      let aux (where, value_num) (operator, name, value_type) =
        let length = String.length where in
        if length = 0 && operator != None
        then raise (Invalid_argument "The first operator should be None");
        if length > 0 && operator = None
        then raise (Invalid_argument "Only the first operator should be None");
        let sep = string_of_operator operator in
        let where = where ^ sep ^ name in
        match value_type with
        | Depend dep -> where ^ " = " ^ dep, value_num
        | Value      -> where_value_gen where "IN" value_num
        | Values     -> where_value_gen where "SIMILAR TO" value_num
      in
      let where, _ = List.fold_left aux ("", value_num + 1) conditions in
      if String.length where = 0 then "" else " WHERE " ^ where

  end

  let select format tables conditions max_size =
    if List.length tables < 1
    then raise (Invalid_argument "from table could not be null");
    let select = "SELECT " ^ (Util.string_of_format format) in
    let from = " FROM " ^ (String.concat ", " tables) in
    let where = Util.where_generator 0 conditions in
    let limit = " LIMIT " ^ (string_of_int max_size) in
    select ^ from ^ where ^ limit

  let insert format table returns =
    let str_format = Util.string_of_format format in
    let insert = "INSERT INTO "^table^" ("^str_format^")" in
    let values = " VALUES " ^ (Util.dollar_generator 1 (List.length format)) in
    let returning = " RETURNING " ^ (String.concat ", " returns) in
    insert ^ values ^ returning

  let update format table conditions returns =
    let update = "UPDATE " ^ table in
    let format_length = List.length format in
    let str_format = Util.string_of_format format in
    let dollars = (Util.dollar_generator 1 format_length) in
    let values = " SET (" ^  str_format ^ ") = " ^ dollars in
    let where = Util.where_generator format_length conditions in
    let returning = " RETURNING " ^ (String.concat ", " returns) in
    update ^ values ^ where ^ returning

  let delete table conditions returns =
    let delete = "DELETE FROM " ^ table in
    let where = Util.where_generator 0 conditions in
    let returning = " RETURNING " ^ (String.concat ", " returns) in
    delete ^ where ^ returning

  let union query1 query2 max_size =
    let limit = " LIMIT " ^ (string_of_int max_size) in
    "("^query1^") UNION ("^query2^ ")" ^ limit

end

(******************************************************************************
*********************************** Content ***********************************
*******************************************************************************)

module Content =
struct

  module QueryName =
  struct
    let single_get = "Content.Single-Get"
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

    let single_get () =
      let contitions = Query.([(None, "content_uri", Value)]) in
      Query.select Format.To_get.content [Table.content] contitions Conf.limit

    let list () =
      Query.select Format.To_get.content [Table.content] [] Conf.limit

    let list_by_subject () =
      let contitions =
        Query.([(None, "content.content_uri", Depend "tag.content_uri");
                (And, "subject", Values)])
      in
      Query.select Format.To_get.content Table.([content; tag]) contitions Conf.limit

    let search_by_title_and_summary () =
      let contitions = Query.([(None, "title", Values); (Or, "summary", Values)]) in
      Query.select Format.To_get.content Table.([content]) contitions Conf.limit

    let search_by_subject () =
      let contitions =
        Query.([(None, "content.content_uri", Depend "tag.content_uri");
                (And, "subject", Values)])
      in
      Query.select Format.To_get.content Table.([content; tag]) contitions Conf.limit

    let search () =
      let query1 = search_by_title_and_summary () in
      let query2 = search_by_subject () in
      Query.union query1 query2 Conf.limit

    let insert () =
      let returns = ["content_uri"] in
      Query.insert Format.To_set.content Table.content returns

    let update () =
      let contitions = Query.([(None, "content_uri", Value)]) in
      let returns = ["content_uri"] in
      Query.update Format.To_set.content Table.content contitions returns

    let delete () =
      let contitions = Query.([(None, "content_uri", Values)]) in
      let returns = ["content_uri"] in
      Query.delete Table.content contitions returns

  end

  module Prepare =
  struct

    let list =
      [QueryName.single_get,                  QueryGen.single_get;
       QueryName.list,                        QueryGen.list;
       QueryName.list_by_subject,             QueryGen.list_by_subject;
       QueryName.search_by_title_and_summary, QueryGen.search_by_title_and_summary;
       QueryName.search_by_subject,           QueryGen.search_by_subject;
       QueryName.search,                      QueryGen.search;
       QueryName.insert,                      QueryGen.insert;
       QueryName.update,                      QueryGen.update;
       QueryName.delete,                      QueryGen.delete]

    let all dbh =
      print_endline "Prepare:: All";
      let aux (name, query_gen) = Pg.prepare dbh name (query_gen ()) in
      lwt () = Utils.Lwt_list.iter_s_exc aux list in
      print_endline "Done\n";
      Lwt.return ()

  end

  let single_get dbh content_uri =
    let params = Query.Util.param_generator Query.([Uri content_uri]) in
    lwt results = Pg.execute dbh QueryName.single_get params in
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

  let insert dbh content_uri title summary =
    let params = Query.Util.param_generator
      Query.([Uri content_uri; String title; String summary])
    in
    lwt results = Pg.execute dbh QueryName.insert params in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content_uri (List.hd results))

  let update dbh content_uri title summary =
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

let connect () =
  let dbh = Pg.connect () in
  lwt () = Content.Prepare.all dbh in
  dbh

let main () =
  let dbh = connect () in

  let unit_try func =
    try_lwt
      lwt _ = func () in
      Lwt.return ()
    with Not_found -> (print_endline "Empty"; Lwt.return ())
  in

  let to_uri = Ptype.uri_of_string in

  let list_try func =
    lwt list = func () in
    if List.length list = 0 then print_endline "Empty";
    Lwt.return ()
  in

  let patate_uri = to_uri "http://patate.com" in

  lwt () = unit_try (fun () -> Content.single_get dbh patate_uri) in

  lwt () = unit_try (fun () -> Content.delete dbh [patate_uri]) in

  lwt () = unit_try (fun () -> Content.insert dbh patate_uri "patateee" "awesome") in

  lwt () = unit_try (fun () -> Content.update dbh patate_uri "aubergine" "carotte") in

  lwt () = list_try (fun () -> Content.list_by_subject dbh ["patate"; "carotte"]) in

  lwt () = list_try (fun () -> Content.search dbh ["tate"; "otte"]) in

  print_endline "Done";

  Pg.close dbh

lwt () = main ()
