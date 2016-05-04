(*
** API Services
** This module registre services of API
*)

module Yojson = Yojson.Basic

open Tools

(******************************************************************************
************************************ Tools ************************************
*******************************************************************************)

let map func = function
  | Some x -> Some (func x)
  | None   -> None

module Pumgrana_registration =
struct

  (** [empty_fallback path msg]
      Generate an empty fallback which return an error [msg] at [path] *)
  let empty_fallback ~path ~empty_msg =
    let get_params = Eliom_parameter.unit in
    let service = Eliom_service.Http.service ~path ~get_params () in
    let handler () () = return_of_error (Tools.bad_request empty_msg) in
    let _ =  Eliom_registration.String.register ~service handler in
    service

  module Json =
  struct

    (** [post_json fallback]
        Generate a post json service on the [fallback] path.  *)
    let post_register_service ~path ~empty_msg =
      let fallback = empty_fallback ~path ~empty_msg in
      let post_params = Eliom_parameter.raw_post_data in
      Eliom_registration.String.register_post_service ~fallback ~post_params

  end

end

(******************************************************************************
*********************************** Content ***********************************
*******************************************************************************)

module Content =
struct

  (* let _ = *)
  (*   Eliom_registration.String.register_service *)
  (*     ~path:["content"; "detail"] *)
  (*     ~get_params:Eliom_parameter.(suffix (string "content_uri")) *)
  (*     (fun content_uri () -> return_of_json (Api.Content.get_detail content_uri)) *)

  let _ =
    Eliom_registration.String.register_service
      ~path:["content"; "list"]
      ~get_params:Eliom_parameter.unit
      (fun () () -> return_of_json (Api.Content.list ()))

  let _ =
    Eliom_registration.String.register_service
      ~path:["content"; "search"]
      ~get_params:Eliom_parameter.(suffix (string "research"))
      (fun field () ->
        return_of_json (Api.Content.search field))

  let _ =
    Pumgrana_registration.Json.post_register_service
      ~path:["content"; "insert"]
      ~empty_msg:"title, summary and text parameters are mandatory"
      (fun () (input_type, ostream) ->
        let aux () =
          lwt yojson = Tools.json_of_ocsigen_string_stream input_type ostream in
          let uri, title, summary, subjects =
            Deserialize.get_insert_content_data yojson
          in
          return_of_json (Api.Content.insert uri title summary subjects)
        in
        Tools.manage_bad_request aux)

  let _ =
    Pumgrana_registration.Json.post_register_service
      ~path:["content"; "delete"]
      ~empty_msg:"contents_uri parameter is mandatory"
      (fun () (input_type, ostream) ->
        let aux () =
          lwt yojson = Tools.json_of_ocsigen_string_stream input_type ostream in
          let contents_uri = Deserialize.get_delete_contents_data yojson in
          return_of_json (Api.Content.delete contents_uri)
        in
        Tools.manage_bad_request aux)

end

(******************************************************************************
************************************* Tag *************************************
*******************************************************************************)

module Tag =
struct

  let _ =
    Eliom_registration.String.register_service
      ~path:["tag"; "search"]
      ~get_params:Eliom_parameter.(suffix (string "search"))
      (fun search () ->
        return_of_json (Api.Tag.search search))

  let _ =
    Eliom_registration.String.register_service
      ~path:["tag"; "from_content"]
      ~get_params:Eliom_parameter.(suffix (string "content_uri"))
      (fun (content_uri) () ->
        let uri = Ptype.uri_decode content_uri in
        return_of_json (Api.Tag.list_from_content uri))

end

(******************************************************************************
********************************* LinkedContent *******************************
*******************************************************************************)

module LinkedContent =
struct

  let _ =
    Eliom_registration.String.register_service
      ~path:["linkedcontent"; "detail"]
      ~get_params:Eliom_parameter.(suffix (string "link_uri"))
      (fun link_uri () ->
        let uri = int_of_string link_uri in
        return_of_json (Api.LinkedContent.get_detail uri))

  let _ =
    Eliom_registration.String.register_service
      ~path:["linkedcontent"; "from_content"]
      ~get_params:Eliom_parameter.(suffix (string "content_uri"))
      (fun content_uri () ->
        let content_uri_dcd = Ptype.uri_decode content_uri in
        return_of_json (Api.LinkedContent.list_from_content content_uri_dcd))

  let _ =
    Eliom_registration.String.register_service
      ~path:["linkedcontent"; "from_content_tags"]
      ~get_params:Eliom_parameter.(suffix ((string "content_uri") **
                                              (list "tags" (string "subject"))))
      (fun (content_uri, subjects) () ->
        let content_uri_dcd = Ptype.uri_decode content_uri in
        return_of_json
          (Api.LinkedContent.list_from_content_tags content_uri_dcd subjects))

  let _ =
    Eliom_registration.String.register_service
      ~path:["linkedcontent"; "search"]
      ~get_params:Eliom_parameter.(suffix ((string "content_uri") **
                                              (string "research")))
      (fun (content_uri, research) () ->
        let uri_dcd = Ptype.uri_decode content_uri in
        return_of_json (Api.LinkedContent.search uri_dcd research))

end

(******************************************************************************
************************************* Link ************************************
*******************************************************************************)

module Link =
struct

  let _ =
    Pumgrana_registration.Json.post_register_service
      ~path:["link"; "insert"]
      ~empty_msg:"All parameters are mandatory"
      (fun () (input_type, ostream) ->
        let aux () =
          lwt yojson = Tools.json_of_ocsigen_string_stream input_type ostream in
          let data = Deserialize.get_insert_links_data yojson in
          return_of_json (Api.Link.insert data)
        in
        Tools.manage_bad_request aux)

  let _ =
    Pumgrana_registration.Json.post_register_service
      ~path:["link"; "delete"]
      ~empty_msg:"links_uri parameter is mandatory"
      (fun () (input_type, ostream) ->
        let aux () =
          lwt yojson = Tools.json_of_ocsigen_string_stream input_type ostream in
          let links_uri = Deserialize.get_delete_links_data yojson in
          return_of_json (Api.Link.delete links_uri)
        in
        Tools.manage_bad_request aux)

end

(******************************************************************************
************************************ Click ************************************
*******************************************************************************)

module Click =
struct

(* (\* Click on Link *\) *)
(* let click_onlink = *)
(*   Eliom_service.Http.service *)
(*     ~path:["link"; "click"] *)
(*     ~get_params:Eliom_parameter.(suffix (string "link_id")) () *)

(* let _ = *)
(*   Eliom_registration.String.register *)
(*     ~service:click_onlink *)
(*     (fun str_link_id () -> *)
(*       let aux () = *)
(*         let link_id_dcd = Ptype.uri_decode str_link_id in *)
(*         let link_id = Ptype.link_id_of_string link_id_dcd in *)
(*         return_of_json (Api.click_onlink link_id) *)
(*       in *)
(*       Tools.manage_bad_request aux) *)

(* (\* Back button *\) *)
(* let back_button = *)
(*   Eliom_service.Http.service *)
(*     ~path:["link"; "back_button"] *)
(*     ~get_params:Eliom_parameter.(suffix (string "link_id")) () *)

(* let _ = *)
(*   Eliom_registration.String.register *)
(*     ~service:back_button *)
(*     (fun str_link_id () -> *)
(*       let aux () = *)
(*         let link_id_dcd = Ptype.uri_decode str_link_id in *)
(*         let link_id = Ptype.link_id_of_string link_id_dcd in *)
(*         return_of_json (Api.back_button link_id) *)
(*       in *)
(*       Tools.manage_bad_request aux) *)

end
