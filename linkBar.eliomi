(** [make links]
    Genetate the link bar *)
val make :
  GUI_deserialize.id ->
  (Rdf_store.link_id * GUI_deserialize.id * string * string) list ->
  Html5_types.div Eliom_content.Html5.F.elt
