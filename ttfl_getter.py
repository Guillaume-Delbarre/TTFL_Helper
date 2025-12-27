import requests
import json

TTFL_MAIN_PATH = "https://fantasy.trashtalk.co/"

def get_cookie_header(path: str = "header_cookie.json") -> dict:
    """
    Récupère la valeur du header Cookie depuis un fichier JSON.

    Args:
        path (str): Chemin vers le fichier JSON contenant le header Cookie.
                    Par défaut: 'header_cookie.json'.

    Returns:
        dict: Contenu du fichier JSON sous forme de dictionnaire.
    """
    with open(path, "r", encoding="utf-8") as f :
        cookie_header = json.load(f)
    return cookie_header

def get_history() :
    header = get_cookie_header()
    path = TTFL_MAIN_PATH + "?tpl=historique"

    response = requests.get(path, headers=header)

    with open("reponse.html", "w", encoding="utf-8") as f :
        f.write(response.text)


get_history()