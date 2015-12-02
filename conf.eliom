(**
**   API Configuration file
*)

open Utils

(** {6 API exception} *)
exception Pum_exc of int * string

(** {6 Return values} *)
let return_ok = 200
let return_created = 201
let return_no_content = 204
let return_not_found = 404
let return_internal_error = 500

(** {6 Error string functions / values} *)
let errstr_not_found str = "'" ^ str ^ "' is Not Found"
let errstr_not_objectid str = "'" ^ str ^ "' is not an valid object id."
let errstr_not_expected str = "'" ^ str ^ "' is not the expected value."
let errstr_exist str = "'" ^ str ^ "' already exist."
let errstr_internal_error = "Internal server error"

{shared{

(** {6 Tag types} *)
let link_tag = "LINK"
let content_tag = "CONTENT"

let link_tag_str = "Link"
let content_tag_str = "Content"

}}

(******************************************************************************
********************************* Conf getter *********************************
*******************************************************************************)

module Configuration =
struct

  module Read =
  struct

    let config = Eliom_config.get_config ()

    let block parent name =
      let exists = function
        | Simplexmlparser.Element (n, _, _) when String.compare n name == 0 -> true
        | _ -> false
      in
      let extract = function
        | Simplexmlparser.Element (_, _, b) -> b
        | _ -> raise Not_found
      in
      try extract (List.find exists parent)
      with _ ->
        raise (Ocsigen_extensions.Error_in_config_file
                 (name ^" must be configured"))

    let string parent name =
      let extract = function
        | Simplexmlparser.PCData v -> v
        | _ -> raise Not_found
      in
      try extract (List.hd (block parent name))
      with _ ->
        raise (Ocsigen_extensions.Error_in_config_file
                 (name ^" must be configured"))

    let opt_string parent name =
      try Some (string parent name)
      with _ -> None

    let int parent name =
      try int_of_string (string parent name)
      with _ ->
        raise (Ocsigen_extensions.Error_in_config_file
                 (name ^" must be configured"))

    let float parent name =
      try float_of_string (string parent name)
      with _ ->
        raise (Ocsigen_extensions.Error_in_config_file
                 (name ^" must be configured"))

  end

  module Postgres =
  struct

    let block = Read.block Read.config "postgresql"

    let host = Read.opt_string block "host"
    let user = Read.string block "user"
    let pwd = Read.opt_string block "pwd"
    let db = Read.string block "dbname"
    let max_connections = Read.int block "maxconnections"

    let limit = Read.int block "limit"
    let mark_decimal = Read.float block "markdecimal"
    let mark_precision = Utils.Maths.power 10. mark_decimal

  end

  module Api =
  struct

    let block = Read.block Read.config "api"

    let timeout = Read.float block "timeout"

    let mode = Read.string block "mode"
    let verbose = String.compare mode "verbose" == 0

  end

  module Bot =
  struct

    let block = Read.block Read.config "bot"

    let host = Read.string block "host"
    let deep = Read.int block "deep"

  end


end
