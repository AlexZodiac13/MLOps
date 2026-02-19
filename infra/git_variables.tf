variable "git_repo_url" {
  type        = string
  description = "URL of the Git repository to clone DAGs from (e.g., https://github.com/my/repo.git)"
  default     = "https://github.com/AlexZodiac13/MLOps.git" # Если пусто, используется локальная папка (для совместимости)
}

variable "git_branch" {
  type        = string
  description = "Branch/tag/commit to checkout"
  default     = "project-work"
}
