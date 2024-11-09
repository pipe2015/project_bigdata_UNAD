import argparse
import sys

families = {
    'user_data': {
        'family_value': dict(),
        'data_columns': [
            'User_ID',
            'Session_ID',
            'Timestamp'
        ]
    },
    'page_data': {
        'family_value': dict(),
        'data_columns' : [
            'Page_URL',
            'Time_Spent_seconds',
            'Clicks'
        ]
    },
    'device_data': {
        'family_value': dict(),
        'data_columns': [
            'Browser_Type', 
            'Device_Type'
        ]
    },
    'content_data': {
        'family_value': dict(),
        'data_columns': [
            'Article_Category',
            'Article_Name',
            'Interacted_Elements',
            'Video_Views'
        ]
    },
    'geo_data': {
        'family_value': dict(),
        'data_columns': ['Geo_Location']
    }
}

# Función principal para manejar los argumentos
def main():
    try:
        parser = argparse.ArgumentParser(description="Script con opciones --delete y --connect.")
        
        # Opciones permitidas --delete y --connect
        parser.add_argument('-d', '--delete', action='store_true', help="Ejecuta la función delete.")
        parser.add_argument('-c', '--connect', action='store_true', help="Ejecuta la función connect.")
        
        # Parseo de argumentos
        args, unknown_args = parser.parse_known_args()
        
        # Verifica si hay argumentos no permitidos
        if unknown_args:
            raise ValueError(f"Error: Se han pasado argumentos no permitidos: {unknown_args}")
        
        # Ejecuta las funciones en base a los argumentos recibidos
        if args.delete:
            print("delete", args.delete)
        
        if args.connect:
            print("connect", args.connect, str(type(args.connect).__str__))
        
        print("correct", dict(map(lambda x :
            (x, families[x]['family_value'] if 'family_value' in families[x] else list())
        , families.keys())))

        datas = [b'click_stream']
        datas = map(lambda x: x.decode(), datas)
        ll = 'click_stream'

        if ll in datas:
            print("no hay datos")

    except ValueError as e:
        print(e)
        sys.exit(1)
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()