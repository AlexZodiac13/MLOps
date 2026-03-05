package test

import (
	"testing"
	"github.com/gruntwork-io/terratest/modules/terraform"
)

func TestTerraformInfrastructure(t *testing.T) {
	t.Parallel()

	terraformOptions := terraform.WithDefaultRetryableErrors(t, &terraform.Options{
		// Путь до папки с конфигурацией Terraform
		TerraformDir: "../",

		// Переменные окружения, если нужно
		// EnvVars: map[string]string{
		// 	"TF_VAR_yc_token": "your-token", // передавать лучше через GitLab ENV
		// },
	})

	// Запуск terraform init и terraform plan (без развертывания ресурсов)
	// Это проверяет синтаксис и доступность модулей/провайдеров
	terraform.InitAndPlan(t, terraformOptions)
}