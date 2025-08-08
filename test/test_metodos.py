from datetime import date
# Importa tu clase o módulo donde esté definido fecha_aleatoria
from utils.metodos import fecha_aleatoria  # Cambia 'tu_modulo' por el nombre real de tu archivo sin .py


def test_fecha_aleatoria_rango():
    """Verifica que la fecha generada está dentro del rango permitido."""
    for _ in range(1000):  # Repetimos varias veces por ser aleatorio
        fecha = fecha_aleatoria()
        assert isinstance(fecha, date)
        assert date(2022, 1, 1) <= fecha <= date(2025, 12, 31)


def test_fecha_aleatoria_variedad():
    """Comprueba que el método genera fechas variadas (no siempre la misma)."""
    fechas = {fecha_aleatoria() for _ in range(100)}
    assert len(fechas) > 1, "El método siempre devuelve la misma fecha, revisar aleatoriedad"
