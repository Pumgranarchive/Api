(*
** Deserializer
** This module provide tools to deserialize yojson for API's services.
 *)

module Yojson = Yojson.Basic

open Yojson.Util
open Pjson
open Conf

let to_tag json =
  let subject = member "subject" json in
  let mark = member "mark" json in
  (to_string subject, to_float mark)

(** Get insert content input data *)
let get_insert_content_data json_content =
  try
    let uri = (member "uri" json_content) in
    let title = member "title" json_content in
    let summary = member "summary" json_content in
    let mark = to_float (member "mark" json_content) in
    let tags = member "tags" json_content in
    to_string uri, to_string title, to_string summary, mark,
    List.map to_tag (to_list tags)
  with
  | _ -> raise (Pum_exc (return_not_found, "Bad insert_content format"))

(** Get delete contents input data *)
let get_delete_contents_data json_content =
  try
    let contents_uri = member "contents_uri" json_content in
    List.map to_string (to_list contents_uri)
  with
  | _ -> raise (Pum_exc (return_not_found, "Bad delete_contents format"))

(** Get delete tags input data *)
let get_delete_tags_data json_content =
  try
    let tags_uri = member "tags_uri" json_content in
    List.map to_string (to_list tags_uri)
  with
  | _ -> raise (Pum_exc (return_not_found, "Bad delete_tags format"))

(** Get insert links input data *)
let get_insert_links_data json_content =
  try
    let data = to_list (member "data" json_content) in
    let aux json =
      let origin_uri = member "origin_uri" json in
      let target_uri = member "target_uri" json in
      let nature = member "nature" json in
      let mark = member "mark" json in
      (to_string origin_uri, to_string target_uri,
       to_string nature, to_float mark)
    in
    List.map aux data
  with
  | _ -> raise (Pum_exc (return_not_found, "Bad insert_links format"))

(** Get delete links input data *)
let get_delete_links_data json_content =
  try
    let links_uri = member "links_uri" json_content in
    List.map to_int (to_list links_uri)
  with
  | _ -> raise (Pum_exc (return_not_found, "Bad delete_links format"))
