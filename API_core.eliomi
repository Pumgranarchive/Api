{server{

(**
   {b API Core -
   This Module do request to database and format the return}
*)


(** {6 Content} *)

(** [get_detail content_id]  *)
val get_detail: string -> Yojson.Basic.json Lwt.t

(** [get_detail_by_link link_id]  *)
val get_detail_by_link: string -> Yojson.Basic.json Lwt.t

(** [get_contents filter tags_id]
    Currently, filter is not used,
    because we haven't enought informations in the DB

    Warning: if one tag_id does not exist, no error will be returned,
    but no content neither.
*)
val get_contents: string option -> string list option -> Yojson.Basic.json Lwt.t

(** [insert_content title text tags_id]  *)
val insert_content: string -> string -> string -> string list option ->
  Yojson.Basic.json Lwt.t

(** [update_content content_id title text tags_id]  *)
val update_content: string -> string option -> string option -> string option ->
  string list option -> Yojson.Basic.json Lwt.t

(** [delete_contents content_ids]  *)
val delete_contents: string list -> Yojson.Basic.json Lwt.t


(** {6 Tag} *)

(** [get_tags tags_id]
    Warning: if one tag_id does not exist, no error will be fire. *)
val get_tags: string list -> Yojson.Basic.json Lwt.t

(** [get_tags_by_type tag_type]  *)
val get_tags_by_type: string -> Yojson.Basic.json Lwt.t

(** [get_tags_from_content content_id]
    Warning: if one tag_id does not exist, no error will be fire. *)
val get_tags_from_content: string -> Yojson.Basic.json Lwt.t

(** [get_tags_from_content_link content_id]
    Warning: if a tag_id does not exist no error will be fire. *)
val get_tags_from_content_link: string -> Yojson.Basic.json Lwt.t

(** [insert_tags type_name id_opt subjects]
    Warning: does not return status list, only the id list *)
val insert_tags: string -> string option -> string list ->
  Yojson.Basic.json Lwt.t

(** [delete_tags tags_id]  *)
val delete_tags: string list -> Yojson.Basic.json Lwt.t


(** {6 Link} *)

(** [get_links_from_content content_id]  *)
val get_links_from_content: string -> Yojson.Basic.json Lwt.t

(** [get_links_from_content_tags content_id tags_id]  *)
val get_links_from_content_tags: string -> string list option ->
  Yojson.Basic.json Lwt.t

(** [insert_links id_from ids_to tags_id]  *)
val insert_links: string -> string list -> string list list ->
  Yojson.Basic.json Lwt.t

(** [update_link link_id tags_id]  *)
val update_link: string -> string list -> Yojson.Basic.json Lwt.t

(** [delete_links links_id]  *)
val delete_links: string list -> Yojson.Basic.json Lwt.t

(** [delete_links_from_to origin_id targets_id]
    Temporary service to solve the issue than we have no access to link_id
    in the client side. *)
val delete_links_from_to: string -> string list -> Yojson.Basic.json Lwt.t

}}