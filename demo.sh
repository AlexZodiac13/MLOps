#!/bin/bash

# Демонстрационный скрипт для A/B pipeline

echo "A/B Pipeline Demo"
echo "===================="

echo ""
echo "Этап 1: Обучение базовой модели"
echo "===================================="
python3 src/train.py --model-name demo_classifier --sample-frac 0.05

echo ""
echo "Этап 2: A/B тестирование"
echo "=========================="
python3 src/ab_test.py --model-name demo_classifier --n-iter 20 --sample-frac 0.05 --auto-deploy

echo ""
echo "Демонстрация завершена!"
echo "========================"
echo "Проверьте результаты в MLflow UI:"
echo "Откройте http://localhost:5000"
