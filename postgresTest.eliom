let main () =
  lwt (dbh : 'a PGOCaml_generic.Make(Postgres.Lwt_thread).t) = Postgres.connect () in

(******************************************************************************
*********************************** Content ***********************************
*******************************************************************************)

  let to_uri = Ptype.uri_of_string in

  let succeed name =
    print_endline ("Succeed   ::\t"^name);
    Lwt.return ()
  in

  let string_of_content_diff input output =
    ("(input) " ^ Postgres.Content.to_string input ^ " != " ^
        "(output) " ^ Postgres.Content.to_string output ^ "\n")
  in

  let string_of_uri_diff input output =
    ("(input) " ^ Ptype.string_of_uri input ^ " != " ^
        "(output) " ^ Ptype.string_of_uri output ^ "\n")
  in

  let add_user_mark (uri, title, summary) =
    (uri, title, summary, 0.)
  in

  let uri = to_uri "http://patate.com" in
  let content_1 = (uri, "patateee", "awesome", 0.) in
  let content_2 = (uri, "aubergine", "carotte") in
  let full_content_2 = add_user_mark content_2 in

  let failed name desc =
    print_endline ("Failed    ::\t"^name);
    print_endline "Caused By ::";
    print_endline desc;
    lwt _ = Postgres.Content.delete dbh [uri] in
    exit 0
  in

  let wrap_try name func =
    try_lwt
      lwt _ = func name in
      Lwt.return ()
    with e -> failed name (Printexc.to_string e)
  in

  print_endline "\n### Unitary tests of the Postgres Module ###";

  lwt _ =
    try_lwt Postgres.Content.delete dbh [uri]
    with _ -> Lwt.return [uri]
  in

  lwt () = wrap_try "Content.Insert" (fun name ->
    lwt uri' = Postgres.Content.insert dbh content_1 in
    lwt content' = Postgres.Content.get dbh uri' in
    if Postgres.Content.compare content_1 content' != 0
    then failed name (string_of_content_diff content_1 content')
    else succeed name)
  in

  lwt () = wrap_try "Content.Update" (fun name ->
    lwt uri' = Postgres.Content.update dbh content_2 in
    lwt content' = Postgres.Content.get dbh uri' in
    if Postgres.Content.compare full_content_2 content' != 0
    then failed name (string_of_content_diff full_content_2 content')
    else succeed name)
  in

  lwt () = wrap_try "Content.Get" (fun name ->
    lwt content' = Postgres.Content.get dbh uri in
    if Postgres.Content.compare full_content_2 content' != 0
    then failed name (string_of_content_diff full_content_2 content')
    else succeed name)
  in

  lwt () = wrap_try "Content.List" (fun name ->
    lwt contents = Postgres.Content.list dbh in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Content.Search_by_title_and_summary" (fun name ->
    lwt contents = Postgres.Content.search_by_title_and_summary dbh ["ott"] in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Content.Search" (fun name ->
    lwt contents = Postgres.Content.search dbh ["aub"] in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

(******************************************************************************
************************************* Tag *************************************
*******************************************************************************)

  let uri = to_uri "http://patate.com" in
  let tag_1 = (uri, "Obama", 3.5) in
  let subject = "Barack" in
  let tag_2 = (uri, subject, 12.2) in

  let add_id id (uri, subject, mark) = (id, uri, subject, mark) in

  let id = ref None in

  let set_id new_id =
    match !id with
    | Some l -> id := Some (l@[new_id])
    | None   -> id := Some [new_id]
  in

  let get_id () = match !id with
    | None   -> raise Not_found
    | Some l -> List.hd l
  in

  let get_ids () = match !id with
    | None   -> raise Not_found
    | Some l -> l
  in

  let failed name desc =
    print_endline ("Failed    ::\t"^name);
    print_endline "Caused By ::";
    print_endline desc;
    lwt _ = Postgres.Tag.delete dbh (get_ids ()) in
    lwt _ = Postgres.Content.delete dbh [uri] in
    exit 0
  in

  let wrap_try name func =
    try_lwt
      lwt _ = func name in
      Lwt.return ()
    with e -> failed name (Printexc.to_string e)
  in

  let string_of_tag_diff input output =
    ("(input) " ^ Postgres.Tag.to_string input ^ " != " ^
        "(output) " ^ Postgres.Tag.to_string output ^ "\n")
  in

  let string_of_id_diff input output =
    ("(input) " ^ string_of_int input ^ " != " ^
        "(output) " ^ string_of_int output ^ "\n")
  in

  lwt () = wrap_try "Tag.Insert" (fun name ->
    lwt id' = Utils.Lwt_list.hd (Postgres.Tag.inserts dbh [tag_1]) in
    let () = set_id id' in
    let full_tag_1 = add_id id' tag_1 in
    lwt tag' = Postgres.Tag.get dbh id' in
    if Postgres.Tag.compare full_tag_1 tag' != 0
    then failed name (string_of_tag_diff full_tag_1 tag')
    else succeed name)
  in

  let id = get_id () in
  let full_tag_2 = add_id id tag_2 in

  lwt () = wrap_try "Tag.Update" (fun name ->
    lwt id' = Postgres.Tag.update dbh id tag_2 in
    lwt tag' = Postgres.Tag.get dbh id' in
    if Postgres.Tag.compare full_tag_2 tag' != 0
    then failed name (string_of_tag_diff full_tag_2 tag')
    else succeed name)
  in

  lwt () = wrap_try "Tag.Get" (fun name ->
    lwt tag' = Postgres.Tag.get dbh id in
    if Postgres.Tag.compare full_tag_2 tag' != 0
    then failed name (string_of_tag_diff full_tag_2 tag')
    else succeed name)
  in

  lwt () = wrap_try "Tag.List" (fun name ->
    lwt tags = Postgres.Tag.list dbh in
    if List.length tags == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Tag.Search" (fun name ->
    lwt tags = Postgres.Tag.search dbh ["ara"] in
    if List.length tags == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Tag.List_by_content_uri" (fun name ->
    lwt tags = Postgres.Tag.list_by_content_uri dbh uri in
    if List.length tags == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Content.List_by_subject" (fun name ->
    lwt contents = Postgres.Content.list_by_subject dbh [subject] in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "Content.Search_by_subject" (fun name ->
    lwt contents = Postgres.Content.search_by_subject dbh ["bar"] in
    if List.length contents == 0
    then failed name "List.length == 0"
    else succeed name)
  in

(******************************************************************************
************************************ Link *************************************
*******************************************************************************)

  let title_3 = "Carotte" in
  let summary_3 = "legume" in
  let uri_3 = Ptype.uri_of_string "http://carotte.com" in
  let title_4 = "Aubergine" in
  let summary_4 = "legume" in
  let uri_4 = Ptype.uri_of_string "http://aubergine.com" in
  let nature_1 = "Nothing" in
  let nature_2 = "Potage" in
  let subject_2 = "Salade" in
  let tag_2 = (uri_3, subject_2, 42.2) in

  let content_3 = (uri_3, title_3, summary_3, 0.) in
  let content_4 = (uri_4, title_4, summary_4, 0.) in
  let link_1 = (uri, uri_3, nature_1, 3., 1.) in
  let link_2 = (uri, uri_3, nature_2, 5.) in
  let link_3 = (uri, uri_4, nature_1, 3., 1.) in
  let linked_content_1 = (nature_1, 3., 1., uri_3, title_3, summary_3) in
  let linked_content_2 = (nature_2, 5., 1., uri_3, title_3, summary_3) in
  let linked_content_3 = (nature_2, 5., 1., uri_4, title_4, summary_4) in

  let add_link_id id (nature, mark, user_mark, uri, title, summary) =
    (id, nature, mark, user_mark, uri, title, summary)
  in

  let link_id = ref None in

  let set_link_id new_id = link_id := Some new_id in

  let get_link_id () = match !link_id with
    | None   -> raise Not_found
    | Some x -> x
  in

  let failed name desc =
    print_endline ("Failed    ::\t"^name);
    print_endline "Caused By ::";
    print_endline desc;
    lwt _ = Postgres.Content.delete dbh [uri_3; uri_4] in
    lwt _ = Postgres.Link.delete dbh (get_link_id ()) in
    lwt _ = Postgres.Tag.delete dbh (get_ids ()) in
    lwt _ = Postgres.Content.delete dbh [uri] in
    exit 0
  in

  let wrap_try name func =
    try_lwt
      lwt _ = func name in
      Lwt.return ()
    with e -> failed name (Printexc.to_string e)
  in

  let string_of_link_diff input output =
    ("(input) " ^ Postgres.LinkedContent.to_string input ^ " != " ^
        "(output) " ^ Postgres.LinkedContent.to_string output ^ "\n")
  in

  lwt () = wrap_try "Link.Insert" (fun name ->
    lwt tag_id = Utils.Lwt_list.hd (Postgres.Tag.inserts dbh [tag_2]) in
    let () = set_id tag_id in
    lwt _ = Postgres.Content.insert dbh content_3 in
    lwt _ = Postgres.Content.insert dbh content_4 in
    lwt id' = Postgres.Link.inserts dbh [link_1; link_3] in
    let () = set_link_id id' in
    let id_1 = List.hd id' in
    lwt link' = Postgres.LinkedContent.get dbh id_1 in
    let full_link_1 = add_link_id id_1 linked_content_1 in
    if Postgres.LinkedContent.compare full_link_1 link' != 0
    then failed name (string_of_link_diff full_link_1 link')
    else succeed name)
  in

  let link_id = List.hd (get_link_id ()) in
  let full_link_2 = add_link_id link_id linked_content_2 in

  lwt () = wrap_try "Link.Update" (fun name ->
    lwt id' = Postgres.Link.update dbh link_id link_2 in
    lwt link' = Postgres.LinkedContent.get dbh id' in
    if Postgres.LinkedContent.compare full_link_2 link' != 0
    then failed name (string_of_link_diff full_link_2 link')
    else succeed name)
  in

  lwt () = wrap_try "LinkedContent.Get" (fun name ->
    lwt link' = Postgres.LinkedContent.get dbh link_id in
    if Postgres.LinkedContent.compare full_link_2 link' != 0
    then failed name (string_of_link_diff full_link_2 link')
    else succeed name)
  in

  lwt () = wrap_try "LinkedContent.List" (fun name ->
    lwt links = Postgres.LinkedContent.list dbh in
    if List.length links == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "LinkedContent.List-By-Content-Uri" (fun name ->
    lwt links = Postgres.LinkedContent.list_by_content_uri dbh uri in
    if List.length links == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "LinkedContent.List-By-Content-Tag" (fun name ->
    lwt links = Postgres.LinkedContent.list_by_content_tag dbh uri [subject_2] in
    if List.length links == 0
    then failed name "List.length == 0"
    else succeed name)
  in

  lwt () = wrap_try "LinkedContent.Search" (fun name ->
    lwt links = Postgres.LinkedContent.search dbh uri ["sal"] in
    if List.length links == 0
    then failed name "List.length == 0"
    else succeed name)
  in

(******************************************************************************
********************************** Cleaning ***********************************
*******************************************************************************)

  lwt () = wrap_try "Link.Delete" (fun name ->
    lwt id' = Utils.Lwt_list.hd (Postgres.Link.delete dbh [link_id]) in
    lwt _ =
      try_lwt Postgres.LinkedContent.get dbh id'
      with
       | Not_found -> Lwt.return full_link_2
       | _ -> failed name (string_of_int id ^ ":: Is not deleted")
    in
    if link_id != id'
    then failed name (string_of_id_diff link_id id')
    else succeed name)
  in

  lwt () = wrap_try "Tag.Delete" (fun name ->
    lwt id' = Utils.Lwt_list.hd (Postgres.Tag.delete dbh (get_ids ())) in
    lwt _ =
      try_lwt Postgres.Tag.get dbh id'
      with
       | Not_found -> Lwt.return full_tag_2
       | _ -> failed name (string_of_int id ^ ":: Is not deleted")
    in
    if id != id'
    then failed name (string_of_id_diff id id')
    else succeed name)
  in

  lwt () = wrap_try "Content.Delete" (fun name ->
    lwt uri' = Utils.Lwt_list.hd (Postgres.Content.delete dbh [uri; uri_3]) in
    lwt _ =
      try_lwt Postgres.Content.get dbh uri
      with
       | Not_found -> Lwt.return full_content_2
       | _ -> failed name (Ptype.string_of_uri uri ^ ":: Is not deleted")
    in
    if Ptype.compare_uri uri uri' != 0
    then failed name (string_of_uri_diff uri uri')
    else succeed name)
 in

 print_endline "Done";
 Postgres.close dbh

(******************************************************************************
********************************* Launching ***********************************
*******************************************************************************)

lwt () = main ()
