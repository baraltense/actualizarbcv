import re
import json
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}

def generar_url_bcv(year, trimestre):
    """Genera URL para archivos XLS del BCV"""
    trimestres = {'I': 'a', 'II': 'b', 'III': 'c', 'IV': 'd'}
    year_short = str(year)[-2:]
    letra = trimestres.get(trimestre)
    
    if not letra:
        raise ValueError(f"Trimestre '{trimestre}' no válido")
    
    return f"https://www.bcv.org.ve/sites/default/files/EstadisticasGeneral/2_1_2{letra}{year_short}_smc.xls"

def descargar_trimestre(year, trimestre):
    """Descarga y procesa un trimestre específico"""
    url = generar_url_bcv(year, trimestre)
    
    print(f"📡 Descargando {year} Trimestre {trimestre}...")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()
        
        # Leer Excel
        df = pd.read_excel(BytesIO(response.content))
        
        # Procesar datos
        datos_procesados = {
            "metadata": {
                "year": year,
                "trimestre": trimestre,
                "url_fuente": url,
                "descargado_en": datetime.utcnow().isoformat() + "Z",
                "total_registros": len(df),
                "columnas": list(df.columns)
            },
            "datos": df.fillna('').to_dict('records')
        }
        
        # Guardar archivo individual
        os.makedirs('datos', exist_ok=True)
        archivo = f"datos/{year}_trimestre_{trimestre}.json"
        with open(archivo, 'w', encoding='utf-8') as f:
            json.dump(datos_procesados, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Guardado: {archivo}")
        return {"success": True, "archivo": archivo, "year": year, "trimestre": trimestre}
        
    except Exception as e:
        error_msg = f"Error en {year}-{trimestre}: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def main():
    """Función principal del scraper"""
    print("🚀 Iniciando scraper de datos estadísticos BCV")
    
    # Configuración de qué datos descargar (últimos 2 años)
    año_actual = datetime.now().year
    configuraciones = []
    
    for year in range(año_actual - 1, año_actual + 1):
        for trimestre in ['I', 'II', 'III', 'IV']:
            configuraciones.append({'year': year, 'trimestre': trimestre})
    
    resultados = []
    
    for config in configuraciones:
        resultado = descargar_trimestre(config['year'], config['trimestre'])
        resultados.append(resultado)
    
    # Guardar resumen de ejecución
    resumen = {
        "ultima_actualizacion": datetime.utcnow().isoformat() + "Z",
        "total_solicitudes": len(resultados),
        "exitosos": sum(1 for r in resultados if r['success']),
        "fallidos": sum(1 for r in resultados if not r['success']),
        "detalles": resultados
    }
    
    with open("resumen_ejecucion.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)
    
    print(f"\n📊 Resumen: {resumen['exitosos']}/{resumen['total_solicitudes']} exitosos")
    
    return resumen

if __name__ == "__main__":
    main()
