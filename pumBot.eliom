let bot_api_uri = "http://127.0.0.1:8083"
let default_iteration = "2550"

let launch uris =
  let base_url = bot_api_uri ^ "/run/"^default_iteration^"//" in
  let str_uris = List.map (fun x -> Ptype.uri_encode (Ptype.string_of_uri x)) uris in
  let concat_uris = String.concat "/" str_uris in
  let request_url = base_url ^ concat_uris ^ "/" in
  let request_uri = Uri.of_string request_url in
  (* print_endline request_url; *)
  try (Cohttp_lwt_unix.Client.get request_uri; ())
  with e -> (print_endline (Printexc.to_string e); ())
