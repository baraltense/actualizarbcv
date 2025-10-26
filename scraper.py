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
        
        # Verificar si la respuesta es exitosa
        if response.status_code == 404:
            return {"success": False, "error": "Archivo no encontrado (404)"}
        response.raise_for_status()
        
        # Verificar que el archivo no esté vacío
        if len(response.content) < 1024:
            return {"success": False, "error": "Archivo vacío o muy pequeño"}
        
        # Leer Excel - intentar diferentes engines
        try:
            df = pd.read_excel(BytesIO(response.content), engine='xlrd')
        except Exception as e:
            print(f"⚠️ xlrd falló, intentando openpyxl: {e}")
            try:
                df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
            except Exception as e2:
                return {"success": False, "error": f"No se pudo leer Excel: {e2}"}
        
        if df.empty:
            return {"success": False, "error": "DataFrame vacío"}
        
        print(f"✅ Datos leídos: {df.shape[0]} filas, {df.shape[1]} columnas")
        
        # Procesar datos
        datos_procesados = {
            "metadata": {
                "year": year,
                "trimestre": trimestre,
                "url_fuente": url,
                "descargado_en": datetime.utcnow().isoformat() + "Z",
                "total_registros": len(df),
                "total_columnas": len(df.columns),
                "columnas": [str(col) for col in df.columns]
            },
            "datos": df.where(pd.notnull(df), None).to_dict('records')
        }
        
        # Guardar archivo individual
        os.makedirs('datos', exist_ok=True)
        archivo = f"datos/{year}_trimestre_{trimestre}.json"
        with open(archivo, 'w', encoding='utf-8') as f:
            json.dump(datos_procesados, f, ensure_ascii=False, indent=2)
        
        return {"success": True, "archivo": archivo}
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Error de conexión: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
    except Exception as e:
        error_msg = f"Error procesando: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def main():
    """Función principal"""
    print("🚀 Iniciando scraper BCV")
    print("📦 Dependencias: pandas, xlrd, openpyxl")
    
    # Solo intentar años realistas
    año_actual = datetime.now().year
    configuraciones = []
    
    # Intentar solo los últimos 3 años
    for year in range(2022, año_actual + 1):
        for trimestre in ['I', 'II', 'III', 'IV']:
            # Para año actual, solo trimestres pasados
            if year == año_actual:
                trimestre_actual = ((datetime.now().month - 1) // 3) + 1
                trimestres_posibles = ['I', 'II', 'III', 'IV'][:trimestre_actual]
                if trimestre in trimestres_posibles:
                    configuraciones.append({'year': year, 'trimestre': trimestre})
            else:
                configuraciones.append({'year': year, 'trimestre': trimestre})
    
    print(f"📋 Intentando {len(configuraciones)} archivos...")
    
    resultados = []
    for config in configuraciones:
        resultado = descargar_trimestre(config['year'], config['trimestre'])
        resultados.append(resultado)
        import time
        time.sleep(0.5)  # Pausa corta
    
    # Resumen
    resumen = {
        "ultima_actualizacion": datetime.utcnow().isoformat() + "Z",
        "total_solicitudes": len(resultados),
        "exitosos": sum(1 for r in resultados if r.get('success')),
        "fallidos": sum(1 for r in resultados if not r.get('success')),
        "detalles": resultados
    }
    
    with open("resumen_ejecucion.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)
    
    print(f"\n📊 RESUMEN: {resumen['exitosos']}/{resumen['total_solicitudes']} exitosos")
    
    return resumen

if __name__ == "__main__":
    main()
