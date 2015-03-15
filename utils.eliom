module Str =
struct

  include Str

  let regexps = List.map Str.regexp

  let contains regexp str =
    try ignore (Str.search_forward regexp str 0); true
    with Not_found -> false

  let exists regexps str =
    List.exists (fun r -> contains r str) regexps

end

module List =
struct

  include List

  let map_exc func list =
    let rec aux blist = function
      | []   -> blist
      | h::t ->
        try
          let e = func h in
          aux (e::blist) t
        with e ->
          (print_endline (Printexc.to_string e);
           aux blist t)
    in
    List.rev (aux [] list)

  let limit size l =
    let rec aux bl s = function
      | []   -> bl
      | h::t ->
        if s >= size then bl else
          aux (h::bl) (s + 1) t
    in
    List.rev (aux [] 0 l)

  let merge func l1 l2 =
    let add list e =
      if List.exists (func e) list
      then list
      else e::list
    in
    List.fold_left add l1 l2

  (** Add at the end  *)
  let (@@) list elem =
    list@[elem]

end

module Lwt_list =
struct

  include Lwt_list

  let hd lwt_l =
    lwt l = lwt_l in
    Lwt.return (List.hd l)

  let concat lwt_l =
    lwt l = lwt_l in
    Lwt.return (List.concat l)

  let filter f lwt_l =
    lwt l = lwt_l in
    Lwt.return (List.filter f l)

  let map f lwt_l =
    lwt l = lwt_l in
    Lwt.return (List.map f l)

  let wait x = x

  (** Map list (not Lwt) which catch all exceptions *)
  let map_exc func list =
    let rec aux blist = function
      | []   -> Lwt.return blist
      | h::t ->
        try_lwt
          let e = func h in
          aux (e::blist) t
        with e ->
          let () = print_endline (Printexc.to_string e) in
          aux blist t
    in
    lwt res = aux [] list in
    Lwt.return (List.rev res)

  (** Lwt Map list in synchronised manner which catch all exceptions *)
  let map_s_exc func list =
    let rec aux blist = function
      | []   -> Lwt.return blist
      | h::t ->
        try_lwt
          lwt e = func h in
          aux (e::blist) t
        with e ->
          let () = print_endline (Printexc.to_string e) in
          aux blist t
    in
    lwt res = aux [] list in
    Lwt.return (List.rev res)

  (** Lwt Iter list in synchronised manner which catch all exceptions *)
  let iter_s_exc func list =
    let rec aux = function
      | []   -> Lwt.return ()
      | h::t ->
        lwt () =
          try_lwt func h
          with e -> (print_endline (Printexc.to_string e); Lwt.return ())
        in
        aux t
    in
    aux list


end

module Opt =
struct

  let get_not_null default = function
    | None -> default
    | Some x -> x

end

module Sparql =
struct

  let select vname where =
    if List.length vname = 0
    then raise (Invalid_argument "vname can not be null");
    let vname = List.map ((^) "?") vname in
    let select = String.concat " " vname in
    "SELECT "^select^" WHERE { "^where^" }"

  let insert_data data =
    "INSERT DATA { "^data^" }"

  let delete_data data =
    "DELETE DATA { "^data^" }"

end
