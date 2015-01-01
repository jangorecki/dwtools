#' @title Anonymize
#' @description Mask data using hasing function
#' @param x vector to anonymize
#' @param algo character scalar to be used for \emph{digest} function in \emph{digest} package, read \code{?digest::digest}.
#' @return character vector of hashes, same length as length of \emph{x} argument.
#' @export
#' @example tests/anonymize_examples.R
anonymize <- function(x, algo){
  if(missing(algo)){
    warning("algo argument missing, read ?digest::digest for available algos, function will use fast pseudo anonymize 'crc32', keep in mind it can be easily reversed.")
    algo <- "crc32"
  }
  unq_hashes <- vapply(unique(x), function(object) digest::digest(object, algo=algo), FUN.VALUE="", USE.NAMES=TRUE)
  unname(unq_hashes[x])
}
