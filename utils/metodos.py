from datetime import date, timedelta
import random


def fecha_aleatoria():
    inicio = date(2022, 1, 1)
    fin = date(2025, 12, 31)
    delta = fin - inicio
    return inicio + timedelta(days=random.randint(0, delta.days))