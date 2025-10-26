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
        
        # Verificar si la respuesta es exitosa
        if response.status_code == 404:
            return {"success": False, "error": "Archivo no encontrado (404)"}
        response.raise_for_status()
        
        # Verificar que el archivo no esté vacío
        if len(response.content) < 1024:
            return {"success": False, "error": "Archivo vacío o muy pequeño"}
        
        # Guardar archivo temporalmente para debug
        with open('temp_file.xls', 'wb') as f:
            f.write(response.content)
        print(f"📁 Archivo descargado: {len(response.content)} bytes")
        
        # Leer Excel con openpyxl explícitamente
        try:
            # Forzar openpyxl para archivos .xls
            df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
            print("✅ Archivo leído con openpyxl")
        except Exception as e:
            print(f"❌ Error con openpyxl: {e}")
            # Intentar sin engine específico
            try:
                df = pd.read_excel(BytesIO(response.content))
                print("✅ Archivo leído con engine por defecto")
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
        
        print(f"💾 Guardado: {archivo}")
        return {"success": True, "archivo": archivo}
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Error de conexión: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
    except Exception as e:
        error_msg = f"Error procesando: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}
    finally:
        # Limpiar archivo temporal
        if os.path.exists('temp_file.xls'):
            os.remove('temp_file.xls')

def main():
    """Función principal"""
    print("🚀 Iniciando scraper BCV")
    print("📦 Dependencias: pandas, openpyxl")
    
    # Solo intentar años realistas - menos archivos para debug
    configuraciones = [
        {'year': 2023, 'trimestre': 'I'},
        {'year': 2023, 'trimestre': 'II'},
        {'year': 2023, 'trimestre': 'III'},
        {'year': 2023, 'trimestre': 'IV'},
    ]
    
    print(f"📋 Intentando {len(configuraciones)} archivos...")
    
    resultados = []
    for config in configuraciones:
        resultado = descargar_trimestre(config['year'], config['trimestre'])
        resultados.append(resultado)
        import time
        time.sleep(1)  # Pausa entre requests
    
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
    
    # Mostrar errores específicos
    if resumen['fallidos'] > 0:
        print(f"\n🔍 Errores detectados:")
        for resultado in resultados:
            if not resultado.get('success'):
                print(f"   - {resultado.get('error', 'Error desconocido')}")
    
    return resumen

if __name__ == "__main__":
    main()
