(*
  API Core
  This Module do request to data base and format well to return it in service
 *)

open Utils

(******************************************************************************
************************************ Tools ************************************
*******************************************************************************)

module Uri =
struct

  let of_strings durty_urls =
    try
      let urls = Urlnorm.normalize durty_urls in
      List.map Ptype.uri_of_string urls
    with Ptype.Invalid_uri str_err ->
      raise Conf.(Pum_exc (return_not_found, str_err))

  let of_string durty_url =
    List.hd (of_strings [durty_url])

end

let cut_research str =
  let regex = Str.regexp "[ \t]+" in
  Str.split regex str

let deep_cout str_list =
  List.fold_left (fun c s -> (String.length s) + c) 0 str_list

let is_something_else uri = true

let detail_platforms =
  [(Pyoutube.is_youtube_uri,    Pyoutube.get_youtube_detail);
   (is_something_else,          Preadability.get_readability_detail)]

let rec get_data_from uri = function
  | (condiction, getter)::next ->
    if condiction uri
    then getter uri
    else get_data_from uri next
  | [] -> raise Not_found

(******************************************************************************
**************************** Connection Manager *******************************
*******************************************************************************)

let (>>=) = Lwt.bind
let (^^) a b = a ^ " : " ^ b

module Connector =
struct

  let connections = Queue.create ()

  let exists n =
    let compute p' c' =
      p' >>= fun p -> c' >>= fun (n2, c2) -> Lwt.return (p || n2 == n)
    in
    Queue.fold compute (Lwt.return false) connections

  let put service (n, c) =
    lwt e = exists n in
    if not e then
      begin
        print_endline ("put "^ service ^^
                          (string_of_int n) ^^
                          string_of_int (Queue.length connections));
        Queue.add (Lwt.return (n, c)) connections;
        Lwt.return ()
      end
    else Lwt.return ()

  let timeout service (n, c) =
    Lwt.async (fun () ->
      Lwt_unix.sleep Conf.Configuration.Api.timeout >>= fun () ->
      lwt e = exists n in
      if not e
      then
        begin
          print_endline ("timeout "^ service ^^ (string_of_int n));
          put service (n, c)
        end
      else Lwt.return ())

  let rec get service =
    try
      lwt (n, c) = Queue.take connections in
      print_endline ("get "^ service ^^
                        (string_of_int n) ^^
                        string_of_int (Queue.length connections));
      begin timeout service (n, c); Lwt.return (n, c) end
    with Queue.Empty ->
      print_endline ("empty get");
      Lwt_unix.sleep 0.01 >>= fun () -> get service

  let fill n =
    let connect n = Postgres.connect () >>= fun c -> Lwt.return (n, c) in
    Utils.repeat n (fun n -> Queue.add (connect n) connections)

end

let () = Connector.fill Conf.Configuration.Postgres.maxconnections

(******************************************************************************
*********************************** Content ***********************************
*******************************************************************************)

module Content =
struct

  let full_assoc ((uri, title, summary, user_mark), body) =
    `Assoc [(Tools.uri_field, `String (Ptype.string_of_uri uri));
            (Tools.title_field, `String title);
            (Tools.summary_field, `String summary);
            (Tools.body_field, `String body)]

  let assoc (uri, title, summary, user_mark) =
    `Assoc [(Tools.uri_field, `String (Ptype.string_of_uri uri));
            (Tools.title_field, `String title);
            (Tools.summary_field, `String summary)]

  let get_detail content_str_uri =
    let aux () =
      let uri = Uri.of_string content_str_uri in
      lwt cid, dbh = Connector.get "Content.detail" in
      lwt res = try_lwt
        let lwt_body = Preadability.get_readability_body uri in
        lwt content = Postgres.Content.get dbh uri in
        lwt body = lwt_body in
        Lwt.return (full_assoc (content, body))
      with Not_found -> Lwt.return `Null in
      lwt () = Connector.put "Content.detail" (cid, dbh) in
      Lwt.return res
    in
    Tools.check_return ~param_name:Tools.contents_ret_name aux

  let list () =
    let aux () =
      lwt cid, dbh = Connector.get "Content.list" in
      lwt results = Postgres.Content.list dbh in
      let json = List.map assoc results in
      lwt () = Connector.put "Content.list" (cid, dbh) in
      Lwt.return (`List json)
    in
    Tools.check_return ~param_name:Tools.contents_ret_name aux

  let search research =
    let compressed_search = Str.global_replace (Str.regexp " ") "" research in
    let aux () =
      let research = cut_research research in
      let length = deep_cout research in
      if length <= 2 then Lwt.return `Null else
        lwt cid, dbh = Connector.get "Content.search" in
        lwt results = Postgres.Content.search dbh research in
        let json = List.map assoc results in
        lwt () = Connector.put "Content.search" (cid, dbh) in
        Lwt.return (`List json)
    in
    if (String.length compressed_search == 0)
    then list ()
    else Tools.check_return ~param_name:Tools.contents_ret_name aux

  (*** Setters  *)

  let insert content_str_uri title summary tags =
    let aux () =
      lwt cid, dbh = Connector.get "Content.insert" in
      let uri = Uri.of_string content_str_uri in
      let default_user_mark = 0. in
      let content = (uri, title, summary) in
      let full_content = (uri, title, summary, default_user_mark) in
      lwt returned_uri =
        try_lwt Postgres.Content.insert dbh full_content
        with _ -> Postgres.Content.update dbh content
      in
      let format_tag (subject, mark) = uri, subject, mark in
      let formated_tags = List.map format_tag tags in
      lwt tag_ids = Postgres.Tag.inserts dbh formated_tags in
      lwt () = Connector.put "Content.insert" (cid, dbh) in
      Lwt.return (`String (Ptype.string_of_uri returned_uri))
    in
    Tools.check_return
      ~param_name:Tools.content_id_ret_name
      ~default_return:Conf.return_created aux

  let delete content_uris =
    let aux () =
      lwt cid, dbh = Connector.get "Content.delete" in
      let uris = Uri.of_strings content_uris in
      lwt returned_uris = Postgres.Content.delete dbh uris in
      let json_list = List.map (fun u -> `String (Ptype.string_of_uri u))
        returned_uris
      in
      lwt () = Connector.put "Content.delete" (cid, dbh) in
      Lwt.return (`List json_list)
    in
    Tools.check_return aux

end

(******************************************************************************
************************************* Tag *************************************
*******************************************************************************)

module Tag =
struct

  let format (id, uri, subject, mark) =
    `Assoc [(Tools.tagsid_field, `Int id);
            (Tools.subject_field, `String subject)]

  let list_from_content content_str_uri =
    let aux () =
      lwt cid, dbh = Connector.get "Tag.list_from_content" in
      let content_uri = Uri.of_string content_str_uri in
      lwt tags = Postgres.Tag.list_by_content_uri dbh content_uri in
      if List.length tags = 0 then PumBot.launch [content_uri];
      let result = `List (List.map format tags) in
      lwt () = Connector.put "Tag.list_from_content" (cid, dbh) in
      Lwt.return result
    in
    Tools.check_return ~param_name:Tools.tags_ret_name aux

  let search research =
    let aux () =
      let research = cut_research research in
      let length = deep_cout research in
      if length <= 2 then Lwt.return `Null else
        lwt cid, dbh = Connector.get "Tag.search" in
        lwt tags = Postgres.Tag.search dbh research in
        let json = `List (List.map format tags) in
        lwt () = Connector.put "Tag.search" (cid, dbh) in
        Lwt.return json
    in
    Tools.check_return ~param_name:Tools.tags_ret_name aux

end

(******************************************************************************
********************************* LinkedContent *******************************
*******************************************************************************)

module LinkedContent =
struct

  let full_assoc (link_id, nature, mark, user_mark, uri, title, summary) =
    let str_uri = Ptype.string_of_uri uri in
    `Assoc [(Tools.link_id_ret_name, `Int link_id);
            (Tools.content_id_ret_name, `String str_uri);
            (Tools.content_title_ret_name, `String title);
            (Tools.content_summary_ret_name, `String summary);
            (Tools.nature_ret_name, `String nature)]

  let assoc (link_id, nature, mark, user_mark, uri, title, summary) =
    let str_uri = Ptype.string_of_uri uri in
    `Assoc [(Tools.link_id_ret_name, `Int link_id);
            (Tools.content_id_ret_name, `String str_uri);
            (Tools.content_title_ret_name, `String title);
            (Tools.content_summary_ret_name, `String summary)]

  let get_detail link_id =
    let aux () =
      lwt cid, dbh = Connector.get "LinkedContent.detail" in
      lwt result = Postgres.LinkedContent.get dbh link_id in
      lwt () = Connector.put "LinkedContent.detail" (cid, dbh) in
      Lwt.return (full_assoc result)
    in
    Tools.check_return ~param_name:Tools.links_ret_name aux

  let list_from_content str_content_uri =
    let aux () =
      lwt cid, dbh = Connector.get "LinkedContent.list_from_content" in
      let uri = Uri.of_string str_content_uri in
      lwt results = Postgres.LinkedContent.list_by_content_uri dbh uri in
      let list = List.map assoc results in
      if List.length list = 0 then PumBot.launch [uri];
      lwt () = Connector.put "LinkedContent.list_from_content" (cid, dbh) in
      Lwt.return (`List list)
    in
    Tools.check_return ~param_name:Tools.links_ret_name aux

  let list_from_content_tags str_content_uri subjects =
    let aux () =
      lwt cid, dbh = Connector.get "LinkedContent.list_from_tags" in
      let uri = Uri.of_string str_content_uri in
      lwt results = Postgres.LinkedContent.list_by_content_tag dbh uri subjects in
      let list = List.map assoc results in
      if List.length list = 0 then PumBot.launch [uri];
      lwt () = Connector.put "LinkedContent.list_from_tags" (cid, dbh) in
      Lwt.return (`List list)
    in
    Tools.check_return ~param_name:Tools.links_ret_name aux

  let search content_uri research =
    let compressed_search = Str.global_replace (Str.regexp " ") "" research in
    let aux () =
      lwt cid, dbh = Connector.get "LinkedContent.search" in
      let research = cut_research research in
      let content_uri = Uri.of_string content_uri in
      let length = deep_cout research in
      lwt results =
          if length <= 2 then Lwt.return []
          else Postgres.LinkedContent.search dbh content_uri research
      in
      let json = List.map assoc results in
      lwt () = Connector.put "LinkedContent.search" (cid, dbh) in
      Lwt.return (`List json)
    in
    if (String.length compressed_search == 0)
    then list_from_content content_uri
    else Tools.check_return ~param_name:Tools.links_ret_name aux

end

(******************************************************************************
************************************* Link ************************************
*******************************************************************************)

module Link =
struct

  (* let internal_insert_links data = *)
  (*   let aux () = *)
  (*     let link_of_uri (origin_str_uri, target_str_uri, tags_str_uri, score) = *)
  (*       let data = *)
  (*         Uri.of_string origin_str_uri, *)
  (*         Uri.of_string target_str_uri, *)
  (*         List.map Uri.of_string tags_str_uri, *)
  (*         score *)
  (*       in *)
  (*       data *)
  (*     in *)
  (*     let link_list = List.map link_of_uri data in *)
  (*     lwt links_id = Rdf_store.insert_links link_list in *)
  (*     let format link_id = *)
  (*       let str_link_id = Rdf_store.string_of_link_id link_id in *)
  (*       `Assoc [(Tools.uri_field, `String str_link_id)] *)
  (*     in *)
  (*     let json_link_id = List.map format links_id in *)
  (*     Lwt.return (`List json_link_id) *)
  (*   in *)
  (*   Tools.check_return *)
  (*     ~default_return:Conf.return_created *)
  (*     ~param_name:Tools.linksid_ret_name *)
  (*     aux *)

  let insert links =
    let aux () =
      lwt cid, dbh = Connector.get "Link.insert" in
      let format (str_origin_uri, str_target_uri, nature, mark) =
        let uris = Uri.of_strings [str_origin_uri; str_target_uri] in
        let origin_uri = List.nth uris 0 in
        let target_uri = List.nth uris 1 in
        (origin_uri, target_uri, nature, mark, 0.)
      in
      (* lwt lwt_link_ids = Lwt_list.map_exc one links in *)
      (* lwt link_ids = Lwt_list.map_s_exc Lwt_list.wait lwt_link_ids in *)
      let formated_links = List.map format links in
      lwt link_ids = Postgres.Link.inserts dbh formated_links in
      let list = List.map (fun x -> `Int x) link_ids in
      lwt () = Connector.put "Link.insert" (cid, dbh) in
      Lwt.return (`List list)
    in
    Tools.check_return ~param_name:Tools.links_ret_name aux

  let delete links_id =
    let aux () =
      lwt cid, dbh = Connector.get "Link.delete" in
      lwt links_id = Postgres.Link.delete dbh links_id in
      let list = List.map (fun x -> `Int x) links_id in
      lwt () = Connector.put "Link.delete" (cid, dbh) in
      Lwt.return (`List list)
    in
    Tools.check_return aux

end

(******************************************************************************
************************************ Click ************************************
*******************************************************************************)

module Click =
struct

  (* let click_onlink link_id = *)
  (*   let aux () = *)
  (*     lwt () = Nosql_store.click_onlink link_id in *)
  (*     Lwt.return `Null *)
  (*   in *)
  (*   Tools.check_return aux *)

  (* let back_button link_id = *)
  (*   let aux () = *)
  (*     lwt () = Nosql_store.back_button link_id in *)
  (*     Lwt.return `Null *)
  (*   in *)
  (*   Tools.check_return aux *)

end
