import json
import glob
import os
from datetime import datetime

def crear_resumen_estadisticas():
    """Crea un archivo resumen con todos los datos disponibles"""
    archivos = glob.glob("datos/*.json")
    datos_combinados = {
        "ultima_actualizacion": datetime.utcnow().isoformat() + "Z",
        "total_archivos": len(archivos),
        "datos_disponibles": [],
        "resumen_por_año": {}
    }
    
    for archivo in archivos:
        try:
            with open(archivo, 'r', encoding='utf-8') as f:
                datos = json.load(f)
            
            # Extraer información del archivo
            year = datos['metadata']['year']
            trimestre = datos['metadata']['trimestre']
            
            # Agregar a datos disponibles
            datos_combinados['datos_disponibles'].append({
                "archivo": archivo,
                "year": year,
                "trimestre": trimestre,
                "registros": datos['metadata']['total_registros'],
                "columnas": len(datos['metadata']['columnas'])
            })
            
            # Agrupar por año
            if year not in datos_combinados['resumen_por_año']:
                datos_combinados['resumen_por_año'][year] = {
                    "trimestres_disponibles": [],
                    "total_registros": 0
                }
            
            datos_combinados['resumen_por_año'][year]["trimestres_disponibles"].append(trimestre)
            datos_combinados['resumen_por_año'][year]["total_registros"] += datos['metadata']['total_registros']
            
        except Exception as e:
            print(f"⚠️ Error procesando {archivo}: {e}")
    
    # Ordenar datos disponibles
    datos_combinados['datos_disponibles'].sort(key=lambda x: (x['year'], x['trimestre']), reverse=True)
    
    # Guardar resumen
    with open("datos/resumen_estadisticas.json", "w", encoding="utf-8") as f:
        json.dump(datos_combinados, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Resumen creado con {len(archivos)} archivos procesados")
    return datos_combinados

if __name__ == "__main__":
    crear_resumen_estadisticas()
