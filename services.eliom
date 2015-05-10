(*
** API Services
** This module registre services of API
*)

open Tools

module Yojson = Yojson.Basic

(******************************************************************************
************************************ Tools ************************************
*******************************************************************************)

let map func = function
  | Some x -> Some (func x)
  | None   -> None

(** [empty_fallback path msg]
    Generate an empty fallback which return an error [msg] at [path] *)
let empty_fallback path msg =
  let get_params = Eliom_parameter.unit in
  let service = Eliom_service.Http.service ~path ~get_params () in
  let handler () () = return_of_error (Tools.bad_request msg) in
  let _ =  Eliom_registration.String.register ~service handler in
  service

(** [post_json fallback]
    Generate a post json service on the [fallback] path.  *)
let post_json fallback =
  let post_params = Eliom_parameter.raw_post_data in
  Eliom_service.Http.post_service ~fallback ~post_params ()

(******************************************************************************
*********************************** Content ***********************************
*******************************************************************************)

module Content =
struct

  let get_detail =
    Eliom_service.Http.service
      ~path:["content"; "detail"]
      ~get_params:Eliom_parameter.(suffix (string "content_uri"))
      ()

  let _ =
    Eliom_registration.String.register
      ~service:get_detail
      (fun content_uri () -> return_of_json (Api.Content.get_detail content_uri))

  let get_contents =
    Eliom_service.Http.service
      ~path:["content"; "list"]
      ~get_params:Eliom_parameter.unit
      ()

  let _ =
    Eliom_registration.String.register
      ~service:get_contents
      (fun () () ->
        return_of_json (Api.Content.list ()))

  let research_contents =
    Eliom_service.Http.service
      ~path:["content"; "research"]
      ~get_params:Eliom_parameter.(suffix (string "research"))
      ()

  let _ =
    Eliom_registration.String.register
      ~service:research_contents
      (fun research () ->
        return_of_json (Api.Content.search research))

  let fallback_insert_content =
    empty_fallback ["content"; "insert"]
      "title, summary and text parameters are mandatory"

  let insert_content_json = post_json fallback_insert_content

  let _ =
    Eliom_registration.String.register
      ~service:insert_content_json
      (fun () (input_type, ostream) ->
        let aux () =
          lwt yojson = Tools.json_of_ocsigen_string_stream input_type ostream in
          let uri, title, summary, mark, subjects =
            Deserialize.get_insert_content_data yojson
          in
          return_of_json (Api.Content.insert uri title summary mark subjects)
        in
        Tools.manage_bad_request aux)

  let fallback_delete_contents =
    empty_fallback ["content"; "delete"]
      "contents_uri parameter is mandatory"

  let delete_contents_json = post_json fallback_delete_contents

  let _ =
    Eliom_registration.String.register
      ~service:delete_contents_json
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

  let get_tags_from_research =
    Eliom_service.Http.service
      ~path:["tag"; "research"]
      ~get_params:Eliom_parameter.(suffix (string "research"))
      ()

  let _ =
    Eliom_registration.String.register
      ~service:get_tags_from_research
      (fun research () ->
        return_of_json (Api.Tag.search research))

  let get_tags_from_content =
    Eliom_service.Http.service
      ~path:["tag"; "from_content"]
      ~get_params:Eliom_parameter.(suffix (string "content_uri"))
      ()

  let _ =
    Eliom_registration.String.register
      ~service:get_tags_from_content
      (fun (content_uri) () ->
        let uri = Ptype.uri_decode content_uri in
        return_of_json (Api.Tag.list_from_content uri))

end

(******************************************************************************
********************************* LinkedContent *******************************
*******************************************************************************)

module LinkedContent =
struct

  let get_detail =
    Eliom_service.Http.service
      ~path:["link"; "detail"]
      ~get_params:Eliom_parameter.(suffix (string "link_uri"))
      ()

  let _ =
    Eliom_registration.String.register
      ~service:get_detail
      (fun link_uri () ->
        let uri = int_of_string link_uri in
        return_of_json (Api.LinkedContent.get_detail uri))

  let get_links_from_content =
    Eliom_service.Http.service
      ~path:["link"; "from_content"]
      ~get_params:Eliom_parameter.(suffix (string "content_uri"))
      ()

  let _ =
    Eliom_registration.String.register
      ~service:get_links_from_content
      (fun content_uri () ->
        let content_uri_dcd = Ptype.uri_decode content_uri in
        return_of_json (Api.LinkedContent.list_from_content content_uri_dcd))

  let get_links_from_content_tags =
       Eliom_service.Http.service
         ~path:["link"; "from_content_tags"]
         ~get_params:Eliom_parameter.(suffix ((string "content_uri") **
            (list "tags" (string "subject"))))
         ()

  let _ =
    Eliom_registration.String.register
     ~service:get_links_from_content_tags
      (fun (content_uri, subjects) () ->
        let content_uri_dcd = Ptype.uri_decode content_uri in
        return_of_json
          (Api.LinkedContent.list_from_content_tags content_uri_dcd subjects))

  let get_links_from_research =
       Eliom_service.Http.service
         ~path:["link"; "from_research"]
         ~get_params:Eliom_parameter.(suffix ((string "content_uri") **
            (string "research")))
         ()

  let _ =
    Eliom_registration.String.register
     ~service:get_links_from_research
      (fun (content_uri, research) () ->
        let uri_dcd = Ptype.uri_decode content_uri in
        return_of_json (Api.LinkedContent.search uri_dcd research))

end

(******************************************************************************
************************************* Link ************************************
*******************************************************************************)

module Link =
struct

  (* Insert links *)
  let fallback_insert_links =
    empty_fallback ["link"; "insert"] "All parameters are mandatory"

  let insert_links_json = post_json fallback_insert_links

  let _ =
    Eliom_registration.String.register
      ~service:insert_links_json
      (fun () (input_type, ostream) ->
        let aux () =
          lwt yojson = Tools.json_of_ocsigen_string_stream input_type ostream in
          let data = Deserialize.get_insert_links_data yojson in
          return_of_json (Api.Link.insert data)
        in
        Tools.manage_bad_request aux)

  let fallback_delete_links =
    empty_fallback ["link"; "delete"] "links_uri parameter is mandatory"

  let delete_links_json = post_json fallback_delete_links

  let _ =
    Eliom_registration.String.register
      ~service:delete_links_json
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
