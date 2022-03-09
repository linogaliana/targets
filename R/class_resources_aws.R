resources_aws_init <- function(
  bucket = NULL,
  prefix = path_objects_dir_cloud(),
  part_size = 5 * (2 ^ 20),
  config_aws = NULL
) {
  resources_aws_new(
    bucket = bucket,
    prefix = prefix,
    part_size = part_size,
    config_aws = config_aws
  )
}

resources_aws_new <- function(
  bucket = NULL,
  prefix = NULL,
  part_size = NULL,
  config_aws = NULL
) {
  force(bucket)
  force(prefix)
  force(part_size)
  force(config_aws)
  enclass(environment(), c("tar_resources_aws", "tar_resources"))
}

#' @export
resources_validate.tar_resources_aws <- function(resources) {
  tar_assert_scalar(resources$bucket %|||% "bucket")
  tar_assert_chr(resources$bucket %|||% "bucket")
  tar_assert_nzchar(resources$bucket %|||% "bucket")
  tar_assert_scalar(resources$prefix)
  tar_assert_chr(resources$prefix)
  tar_assert_nzchar(resources$prefix)
  tar_assert_scalar(resources$part_size %|||% 1e8)
  tar_assert_dbl(resources$part_size %|||% 1e8)
  tar_assert_positive(resources$part_size %|||% 1e8)
  tar_assert_list(resources$config_aws)
}

#' @export
print.tar_resources_aws <- function(x, ...) {
  cat(
    "<tar_resources_aws>\n ",
    paste0(paste_list(as.list(x)), collapse = "\n  ")
  )
}
