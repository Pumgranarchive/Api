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

module Format =
struct

  module To_get =
  struct
    let content = "content.content_uri, title, summary"
    let link = "link_id, origin_uri, target_uri, nature, link.mark, user_mark"
    let tag = "tag_id, tag.content_uri, subject, tag.mark"
  end

  module To_set =
  struct
    let content = "content_uri, title, summary"
    let link = "origin_uri, target_uri, nature, mark, user_mark"
    let tag = "content_uri, subject, tag.mark"
  end

end

(******************************************************************************
************************************ Utils ************************************
*******************************************************************************)

module Row =
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

  let to_content_uri row =
    print 0 row;
    let () = check_size row 1 "content_uri" in
    Ptype.uri_of_string (get row "content.uri" 0)

  let to_content row =
    print 0 row;
    let () = check_size row 3 "content" in
    let uri = Ptype.uri_of_string (get row "content.uri" 0) in
    let title = get row "content.title" 1 in
    let summary = get row "content.summary" 2 in
    uri, title, summary

  let to_link row =
    print 0 row;
    let () = check_size row 6 "link" in
    let id = int_of_string (get row "link.id" 0) in
    let o_uri = Ptype.uri_of_string (get row "link.origin_uri" 1) in
    let t_uri = Ptype.uri_of_string (get row "link.target_uri" 2) in
    let nature = get row "link.nature" 3 in
    let mark = float_of_string (get row "link.mark" 4) in
    let user_mark = float_of_string (get row "link.user_mark" 6) in
    id, o_uri, t_uri, nature, mark, user_mark

  let to_tag row =
    print 0 row;
    let () = check_size row 4 "tag" in
    let id = int_of_string (get row "tag.id" 0) in
    let uri = Ptype.uri_of_string (get row "tag.content_uri" 1) in
    let subject = get row "tag.subject" 2 in
    let mark = float_of_string (get row "tag.mark" 3) in
    id, uri, subject, mark

end

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

  let execute lwt_dbh (query, params) =
    print_endline query;
    List.iter (fun x -> match x with | Some x -> print_endline x | _ -> ()) params;
    lwt dbh = lwt_dbh in
    let name = string_of_int (Random.int 10000 + 10000) in
    lwt () = PGOCaml.prepare dbh ~query ~name () in
    PGOCaml.execute dbh ~name ~params ()

  type where =
  | Value of string
  | Values of string list
  | Contains of string list
  | Depend of string

  type operator = None | And | Or

  type condition = (operator * string * where) list

  let string_of_operator = function
    | None  -> ""
    | And   -> " AND "
    | Or    -> " OR "

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

  let param_generator (where, params) values =
    let var_num = List.length params + 1 in
    let option_params = List.rev (List.map (fun v -> Some v) values) in
    let dollar = dollar_generator var_num (List.length values) in
    (where ^ dollar, params @ option_params)

  let where_generator data conditions =
    let aux (where, params) (operator, name, w_value) =
      let length = String.length where in
      if length = 0 && operator != None
      then raise (Invalid_argument "The first operator should be None");
      if length > 0 && operator = None
      then raise (Invalid_argument "Only the first operator should be None");
      let sep = string_of_operator operator in
      let where = where ^ sep ^ name in
      let to_regexp modulo strs =
        let regexp = "(" ^ (String.concat "|" strs) ^ ")" in
        if modulo then String.concat "" ["%"; regexp; "%"] else regexp
      in
      match w_value with
      | Depend v -> (where ^ " = " ^ v, params)
      | Value v -> param_generator (where ^ " IN ", params) [v]
      | Values vs ->
        param_generator (where ^ " SIMILAR TO ", params) [to_regexp false vs]
      | Contains vs ->
        param_generator (where ^ " SIMILAR TO ", params) [to_regexp true vs]
    in
    let where, params = List.fold_left aux data conditions in
    let where = if String.length where > 0 then " WHERE " ^ where else "" in
    (where, params)

  let select format tables conditions =
    if List.length tables < 1
    then raise (Invalid_argument "from table could not be null");
    let empty = ("", []) in
    let select = "SELECT " ^ format in
    let from = " FROM " ^ (String.concat ", " tables) in
    let where, params = where_generator empty conditions in
    let query = select ^ from ^ where in
    (query, params)

  let insert format table p_values returns =
    let insert = "INSERT INTO " ^ table ^ " (" ^ format ^ ")" in
    let values = " VALUES " ^ (dollar_generator 1 (List.length p_values)) in
    let returning = " RETURNING " ^ (String.concat ", " returns) in
    let params = List.map (fun x -> Some x) p_values in
    let query = insert ^ values ^ returning in
    (query, params)

  let update format table p_values conditions returns =
    let update = "UPDATE " ^ table in
    let dollars = (dollar_generator 1 (List.length p_values)) in
    let params = List.map (fun x -> Some x) p_values in
    let values = " SET (" ^  format ^ ") = " ^ dollars in
    let where, params' = where_generator ("", params) conditions in
    let returning = " RETURNING " ^ (String.concat ", " returns) in
    let query = update ^ values ^ where ^ returning in
    (query, params')

  let delete table conditions returns =
    let delete = "DELETE FROM " ^ table in
    let where, params = where_generator ("", []) conditions in
    let returning = " RETURNING " ^ (String.concat ", " returns) in
    let query = delete ^ where ^ returning in
    (query, params)

end

(******************************************************************************
*********************************** Content ***********************************
*******************************************************************************)

module Content =
struct

  let single_get dbh content_uri =
    let contitions = Pg.([(None, "content_uri", Value content_uri)]) in
    let tables = [Table.content] in
    let query = Pg.select Format.To_get.content tables contitions in
    lwt results = Pg.execute dbh query in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content (List.hd results))

  let list dbh =
    let query = Pg.select Format.To_get.content [Table.content] [] in
    lwt results = Pg.execute dbh query in
    Lwt.return (List.map Row.to_content results)

  let list_by_subjects dbh subjects =
    let contitions =
      Pg.([(None, "content.content_uri", Depend "tag.content_uri");
           (And, "subject", Values subjects)])
    in
    let tables = Table.([content; tag]) in
    let query = Pg.select Format.To_get.content tables contitions in
    lwt results = Pg.execute dbh query in
    Lwt.return (List.map Row.to_content results)

  let search dbh researchs =
    let contitions = Pg.([(None, "title", Contains researchs);
                          (Or, "summary", Contains researchs)])
    in
    let tables = Table.([content]) in
    let query = Pg.select Format.To_get.content tables contitions in
    lwt results = Pg.execute dbh query in
    Lwt.return (List.map Row.to_content results)

  let search_by_subjects dbh researchs =
    let contitions =
      Pg.([(None, "content.content_uri", Depend "tag.content_uri");
           (And, "subject", Contains researchs)])
    in
    let tables = Table.([content; tag]) in
    let query = Pg.select Format.To_get.content tables contitions in
    lwt results = Pg.execute dbh query in
    Lwt.return (List.map Row.to_content results)

  let insert dbh content_uri title summary =
    let values = [content_uri; title; summary] in
    let returns = ["content_uri"] in
    let query = Pg.insert Format.To_set.content Table.content values returns in
    lwt results = Pg.execute dbh query in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content_uri (List.hd results))

  let update dbh content_uri title summary =
    let format = Format.To_set.content in
    let values = [content_uri; title; summary] in
    let contitions = Pg.([(None, "content_uri", Value content_uri)]) in
    let returns = ["content_uri"] in
    let query = Pg.update format Table.content values contitions returns in
    lwt results = Pg.execute dbh query in
    if List.length results = 0 then raise Not_found
    else Lwt.return (Row.to_content_uri (List.hd results))

  let delete dbh content_uris =
    let contitions = Pg.([(None, "content_uri", Values content_uris)]) in
    let returns = ["content_uri"] in
    let query = Pg.delete Table.content contitions returns in
    lwt results = Pg.execute dbh query in
    Lwt.return (List.map Row.to_content_uri results)

end

let main () =
  let dbh = Pg.connect () in
  (* try *)
  (*   lwt content = Content.single_get dbh "patate" in *)
  (*   print_endline "Found"; *)
  (*   Lwt.return () *)
  (* with e -> print_endline "Empty"; *)

  lwt content = Content.delete dbh ["http://patate.com"] in

  lwt content = Content.insert dbh "http://patate.com" "patateee" "awesome" in

  (* lwt content = Content.update dbh "http://patate.com" "aubergine" "boom" in *)

  lwt contents = Content.list_by_subjects dbh ["patate"; "carotte"] in
  if List.length contents = 0 then print_endline "Empty";

  lwt contents = Content.search dbh ["tat"; "carotte"] in
  if List.length contents = 0 then print_endline "Empty";

  lwt contents = Content.search_by_subjects dbh ["patate"; "carotte"] in
  if List.length contents = 0 then print_endline "Empty";

  Pg.close dbh

lwt () = main ()
