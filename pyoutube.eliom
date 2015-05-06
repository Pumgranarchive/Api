open Utils

exception Pyoutube of string

(******************************************************************************
********************************** Utils **************************************
*******************************************************************************)

let is_youtube_uri uri =
  let str_uri = Ptype.string_of_uri uri in
  try ignore (Youtube_http.get_video_id_from_url str_uri); true
  with _ -> false

let id_of_uri uri =
  Youtube_http.get_video_id_from_url (Ptype.string_of_uri uri)

let format (_, title, str_uri, summary, _) =
  let uri = Ptype.uri_of_string str_uri in
  uri, title, summary

let log uri exc detail =
  let suri = Ptype.string_of_uri uri in
  let title = Log.log "youtube.error" suri exc detail in
  raise (Pyoutube title)

let listenner uri =
  try_lwt
    let id = id_of_uri uri in
    lwt videos = Youtube_http.get_videos_from_ids [id] in
    let new_data_list = List.map format videos in
    let new_data = List.hd new_data_list in
    Lwt.return new_data
  with exc -> log uri exc ""

lwt cash = Cash.make "Youtube" listenner

(******************************************************************************
******************************** Funtions *************************************
*******************************************************************************)

let get_youtube_triple uris =
  lwt know_uris = Lwt_list.filter_p (Cash.exists cash) uris in
  lwt unknow_uris = Lwt_list.filter_p (Cash.not_exists cash) uris in
  let know_data = List.map (Cash.get cash) know_uris in
  let ids = List.map id_of_uri unknow_uris in
  let add lwt_data =
    lwt data = lwt_data in
    let uri, title, summary = data in
    Cash.add cash uri data
  in
  let lwt_format data = Lwt.return (format data) in
  lwt new_data =
      if List.length ids > 0
      then
        lwt videos = Youtube_http.get_videos_from_ids ids in
        lwt new_data = Lwt_list.map_exc lwt_format videos in
        lwt () = Lwt_list.iter_s_exc add new_data in
        Lwt.return new_data
      else Lwt.return []
  in
  Lwt.return (know_data@new_data)

let get_youtube_detail uri =
  lwt results = get_youtube_triple [uri] in
  lwt uri, title, summary = List.hd results in
  lwt body = Preadability.get_readability_body uri in
  Lwt.return (uri, title, summary, body, true)
