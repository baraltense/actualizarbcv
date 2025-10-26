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
    print(f"🔗 URL: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        response.raise_for_status()
        
        # Verificar que el archivo no esté vacío
        if len(response.content) < 1024:  # Menos de 1KB probablemente es error
            return {"success": False, "error": f"Archivo demasiado pequeño o vacío: {len(response.content)} bytes"}
        
        # Leer Excel con engine específico para .xls
        try:
            # Primero intentar con xlrd
            df = pd.read_excel(BytesIO(response.content), engine='xlrd')
        except Exception as e:
            print(f"⚠️ xlrd falló, intentando con openpyxl: {e}")
            try:
                # Fallback a openpyxl
                df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
            except Exception as e2:
                print(f"⚠️ openpyxl también falló: {e2}")
                return {"success": False, "error": f"No se pudo leer el archivo Excel: {e2}"}
        
        # Verificar que el DataFrame tenga datos
        if df.empty:
            return {"success": False, "error": "DataFrame vacío - sin datos"}
        
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
        
        print(f"💾 Guardado: {archivo}")
        return {"success": True, "archivo": archivo, "year": year, "trimestre": trimestre}
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Error de conexión: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
    except Exception as e:
        error_msg = f"Error procesando archivo: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def main():
    """Función principal del scraper"""
    print("🚀 Iniciando scraper de datos estadísticos BCV")
    print("📦 Dependencias: pandas, xlrd, openpyxl")
    
    # Configuración de qué datos descargar (solo años con datos probables)
    año_actual = datetime.now().year
    configuraciones = []
    
    # Solo intentar años recientes que probablemente existan
    for year in range(2021, año_actual + 1):
        for trimestre in ['I', 'II', 'III', 'IV']:
            # Para el año actual, solo trimestres pasados
            if year == año_actual:
                trimestre_actual = ((datetime.now().month - 1) // 3) + 1
                trimestres_posibles = ['I', 'II', 'III', 'IV'][:trimestre_actual]
                if trimestre in trimestres_posibles:
                    configuraciones.append({'year': year, 'trimestre': trimestre})
            else:
                configuraciones.append({'year': year, 'trimestre': trimestre})
    
    print(f"📋 Intentando descargar {len(configuraciones)} archivos...")
    
    resultados = []
    
    for config in configuraciones:
        resultado = descargar_trimestre(config['year'], config['trimestre'])
        resultados.append(resultado)
        
        # Pequeña pausa para no saturar el servidor
        import time
        time.sleep(1)
    
    # Guardar resumen de ejecución
    resumen = {
        "ultima_actualizacion": datetime.utcnow().isoformat() + "Z",
        "total_solicitudes": len(resultados),
        "exitosos": sum(1 for r in resultados if r.get('success')),
        "fallidos": sum(1 for r in resultados if not r.get('success')),
        "detalles": resultados
    }
    
    with open("resumen_ejecucion.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)
    
    print(f"\n📊 RESUMEN FINAL:")
    print(f"   ✅ Exitosos: {resumen['exitosos']}")
    print(f"   ❌ Fallidos: {resumen['fallidos']}")
    print(f"   📁 Total: {resumen['total_solicitudes']}")
    
    # Mostrar errores específicos
    if resumen['fallidos'] > 0:
        print(f"\n🔍 Errores detectados:")
        for resultado in resultados:
            if not resultado.get('success'):
                print(f"   - {resultado.get('error', 'Error desconocido')}")
    
    return resumen

if __name__ == "__main__":
    main()
