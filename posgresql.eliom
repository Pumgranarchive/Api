(*
  PosgreSQL
  This module simplify request to PosgreSQL
*)

(******************************************************************************
******************************* Configuration *********************************
*******************************************************************************)

let _ = Random.self_init ()

let (>>=) = Lwt.bind

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

  let content = "content.content_uri, title, summary"
  let link = "link_id, origin_uri, target_uri, nature, link.mark, user_mark"
  let tag = "tag_id, tag.content_uri, subject, tag.mark"

end

(******************************************************************************
************************************ Utils ************************************
*******************************************************************************)

module Pg =
struct

  let connect = PGOCaml.connect
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
    let name = string_of_int (10000 + Random.int 10000) in
    lwt () = PGOCaml.prepare dbh ~query ~name () in
    PGOCaml.execute dbh ~name ~params ()

  type where =
  | List of string list
  | String of string
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
      let var_num = List.length params + 1 in
      if var_num = 1 && operator != None then raise (Invalid_argument "The first operator should be None");
      if var_num > 1 && operator = None then raise (Invalid_argument "Only the first operator should be None");
      let sep = string_of_operator operator in
      let where = where ^ sep in
      match w_value with
      | Depend value -> (where ^ name ^ " = " ^ value, params)
      | String value -> param_generator (where ^ name ^ " IN ", params) [value]
      | List values -> param_generator (where ^ name ^ " IN ", params) values
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

end

(******************************************************************************
*********************************** Content ***********************************
*******************************************************************************)

module Content =
struct

  let single_get dbh content_uri =
    let contitions = Pg.([(None, "content_uri", String content_uri)]) in
    let query = Pg.select Format.content [Table.content] contitions in
    lwt results = Pg.execute dbh query in
    if List.length results = 0 then raise Not_found
    else Lwt.return (List.hd results)

  let list dbh =
    let query = Pg.select Format.content [Table.content] [] in
    Pg.execute dbh query

  let list_by_subjects dbh subjects =
    let contitions = Pg.([(None, "subject", List subjects);
                          (And, "content.content_uri", Depend "tag.content_uri")])
    in
    let query = Pg.select Format.content Table.([content; tag]) contitions in
    Pg.execute dbh query

  let search dbh researchs =
    let contitions = Pg.([(None, "title", List researchs);
                          (Or, "summary", List researchs);
                          (Or, "subject", List researchs);
                          (And, "content.content_uri", Depend "tag.content_uri")])
    in
    let query = Pg.select Format.content Table.([content; tag]) contitions in
    Pg.execute dbh query

end

let main () =
  let dbh = Pg.connect () in
  (* try *)
  (*   lwt content = Content.single_get dbh "patate" in *)
  (*   print_endline "Found"; *)
  (*   Lwt.return () *)
  (* with e -> print_endline "Empty"; *)

  lwt contents = Content.list_by_subjects dbh ["patate"; "carotte"] in
  if List.length contents = 0 then print_endline "Empty";

  lwt contents = Content.search dbh ["patate"; "carotte"] in
  if List.length contents = 0 then print_endline "Empty";
  Pg.close dbh

lwt () = main ()
