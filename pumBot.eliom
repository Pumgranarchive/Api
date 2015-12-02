module Conf = Conf.Configuration

let deep = string_of_int Conf.Bot.deep

let launch uris =
  let base_url = Conf.Bot.host ^ "/run/"^ deep ^"//" in
  let str_uris = List.map (fun x -> Ptype.uri_encode (Ptype.string_of_uri x)) uris in
  let concat_uris = String.concat "/" str_uris in
  let request_url = base_url ^ concat_uris ^ "/" in
  let request_uri = Uri.of_string request_url in
  (* print_endline request_url; *)
  try (Cohttp_lwt_unix.Client.get request_uri; ())
  with e -> (print_endline (Printexc.to_string e); ())
