# **TTFL_Helper - Guide pour les Prompts Efficaces**
*Comment structurer vos demandes pour obtenir des réponses précises et rapides.*

---

## **📌 Pourquoi ce guide ?**
Pour que je puisse vous accompagner **de manière précise et efficace**, il est essentiel de structurer vos demandes de façon claire. Ce guide vous explique comment formuler vos prompts pour que je comprenne rapidement :
- **Le contexte** (quel fichier, quelle tâche).
- **L'objectif** (ce que vous voulez accomplir).
- **Les détails techniques** (erreur, code existant, etc.).

---

## **📌 Format Recommandé pour Vos Prompts**
Utilisez ce template pour structurer vos demandes. Cela me permettra de vous fournir des réponses **ciblées et utiles**.

```markdown
**Contexte** :
- Je travaille sur la tâche **[ID de la tâche, ex: TASK-06]** du projet **TTFL_Helper**.
- Fichier concerné : **[chemin du fichier, ex: `src/api/nba_client.py`]**. 
- Objectif : **[décrivez brièvement l'objectif, ex: "Récupérer les stats des joueurs via `nba_api`"]**.

**Problème/Question** :
- [Décrivez le problème ou posez votre question ici.
  Exemple :
  - "Je n'arrive pas à récupérer les stats des joueurs avec `nba_api`. Voici mon code : ..."
  - "Comment implémenter la méthode `is_available()` dans `Player` pour vérifier le cooldown de 30 jours ?"
  - "J'ai une erreur `ModuleNotFoundError` quand j'essaie d'importer `nba_api`. Comment résoudre ça ?"]

**Code/Erreur** (si applicable) :
```[langage] [chemin/du/fichier]
# Collez ici le code pertinent ou le message d'erreur.
# Exemple :
from nba_api.stats.endpoints import playergamelog

def get_player_stats(player_id):
    return playergamelog.PlayerGameLog(player_id=player_id).get_data_frames()[0]
```

**Attendu** :
- [Décrivez ce que vous attendez comme réponse.
  Exemple :
  - "Un exemple de code fonctionnel pour récupérer les stats."
  - "Une explication sur comment calculer le cooldown."
  - "Une correction pour mon code actuel."]
```

---

## **📌 Exemples Concrets de Prompts**

### **Exemple 1 : Demande d'Aide pour une Tâche Spécifique**
**Contexte** :
- Je travaille sur la **TASK-06** (Créer `nba_client.py`).
- Fichier concerné : `src/api/nba_client.py`.
- Objectif : Récupérer les stats des 5 derniers matchs d'un joueur via `nba_api`.

**Problème/Question** :
Je n'arrive pas à filtrer les 5 derniers matchs d'un joueur. Voici mon code actuel :

```python src/api/nba_client.py
from nba_api.stats.endpoints import playergamelog

def get_last_five_games(player_id):
    gamelog = playergamelog.PlayerGameLog(player_id=player_id)
    df = gamelog.get_data_frames()[0]
    return df.head(5)  # Est-ce que c'est suffisant ?
```

**Attendu** :
- Confirmer si `df.head(5)` est la bonne approche.
- Si non, me montrer comment filtrer les 5 derniers matchs **par date** (et non par ordre d'insertion).

---

### **Exemple 2 : Demande de Correction d'Erreur**
**Contexte** :
- Je travaille sur la **TASK-03** (Implémenter `config.py`).
- Fichier concerné : `src/utils/config.py`.
- Objectif : Définir les chemins pour le cache et la base de données.

**Problème/Question** :
J'ai une erreur `FileNotFoundError` quand j'essaie de créer le dossier `data/cache/`. Voici mon code :

```python src/utils/config.py
import os

DATA_DIR = os.path.join(os.getcwd(), "data")
CACHE_DIR = os.path.join(DATA_DIR, "cache")

os.makedirs(CACHE_DIR, exist_ok=True)  # ça devrait marcher, non ?
```

**Erreur** :
```
FileNotFoundError: [Errno 2] No such file or directory: '/home/user/ttfl_helper/data/cache'
```

**Attendu** :
- Pourquoi cette erreur persiste ?
- Comment corriger le code pour s'assurer que les dossiers sont créés ?

---

### **Exemple 3 : Demande de Validation de Logique Métier**
**Contexte** :
- Je travaille sur la **TASK-14** (Implémenter `selector.py`).
- Fichier concerné : `src/core/selector.py`.
- Objectif : Filtrer les joueurs disponibles (cooldown de 30 jours).

**Problème/Question** :
Voici ma fonction pour vérifier si un joueur est disponible. Est-ce que la logique est correcte ?

```python src/core/selector.py
from datetime import datetime, timedelta

def is_available(player, today):
    if player.last_selected_date is None:
        return True
    cooldown_end = player.last_selected_date + timedelta(days=30)
    return today >= cooldown_end
```

**Attendu** :
- Valider si la logique est bonne.
- Si non, me proposer une correction.

---

### **Exemple 4 : Demande de Clarification sur les Règles Métier**
**Contexte** :
- Je consulte le fichier **PROJECT.md** (section "Règles Métier").
- Objectif : Comprendre la formule de calcul des points TTFL.

**Problème/Question** :
Dans le fichier `PROJECT.md`, il est écrit que les points TTFL sont calculés comme suit :
`points = PTS + REB + AST + STL + BLK - TO`.

Est-ce que cette formule est **officielle** ? Si non, où puis-je trouver la formule exacte ?

**Attendu** :
- Confirmer ou infirmer la formule.
- Si elle n'est pas officielle, me guider vers la source correcte.

---

## **📌 Bonnes Pratiques pour les Prompts**

### **1. Soyez Précis**
- Mentionnez toujours **l'ID de la tâche** (ex: `TASK-06`) et le **fichier concerné**.
- Décrivez **l'objectif** de la tâche en cours.

✅ **Bon** :
> **Contexte** : Je travaille sur la **TASK-08** (Implémenter `cache.py`).

❌ **À éviter** :
> "J'ai un problème avec le cache."

---

### **2. Partagez le Code ou l'Erreur**
- Collez le code **complet** (ou la partie pertinente) dans un bloc de code avec le **langage et le chemin du fichier**.
- Si vous avez une erreur, partagez le **message d'erreur complet**.

✅ **Bon** :
```python src/utils/cache.py
import json

def save_data(data):
    with open("data.json", "w") as f:
        json.dump(data, f)
```

❌ **À éviter** :
> "Mon code ne marche pas, aide-moi."

---

### **3. Dites Ce Que Vous Attendez**
- Exemple : "Je veux un exemple de code", "Je veux une correction", "Je veux une explication".

✅ **Bon** :
> **Attendu** : Une correction pour mon code actuel.

❌ **À éviter** :
> "Fais-moi un code."

---

### **4. Référez-Vous aux Fichiers du Projet**
- Si vous parlez de `PROJECT.md`, dites explicitement :
  *"Dans le fichier `PROJECT.md`, section 'Règles Métier', il est écrit que..."*.
- Si vous parlez de `TASKS.md`, mentionnez l'**ID de la tâche**.

✅ **Bon** :
> Dans le fichier `PROJECT.md`, il est mentionné que la formule des points TTFL est `PTS + REB + AST + STL + BLK - TO`. Est-ce exact ?

❌ **À éviter** :
> "Comment calculer les points ?"

---

## **⚠️ À Éviter Absolument**
- ❌ **"Aide-moi avec mon code"** → Trop vague.
- ❌ **"Ça ne marche pas"** → Pas assez de détails.
- ❌ **"Comment faire pour les stats ?"** → Quel fichier ? Quelle tâche ?
- ❌ **"J'ai une erreur"** → Quelle erreur ? Dans quel fichier ?

---

## **🎯 Résumé des Étapes pour un Prompt Efficace**
1. **Contexte** : Précisez la tâche, le fichier et l'objectif.
2. **Problème/Question** : Décrivez clairement le problème ou la question.
3. **Code/Erreur** : Partagez le code ou l'erreur si applicable.
4. **Attendu** : Dites ce que vous attendez comme réponse.

---

## **📌 Exemple de Prompt Idéal**
```markdown
**Contexte** :
- Je travaille sur la **TASK-08** (Implémenter `cache.py`).
- Fichier concerné : `src/utils/cache.py`.
- Objectif : Sauvegarder les données des joueurs en JSON.

**Problème/Question** :
Voici mon code pour sauvegarder les données. Est-ce que c'est la bonne approche ?

```python src/utils/cache.py
import json
import os
from src.utils.config import CACHE_DIR

def save_to_cache(filename, data):
    filepath = os.path.join(CACHE_DIR, filename)
    with open(filepath, "w") as f:
        json.dump(data, f)
```

**Attendu** :
- Valider si le code est correct.
- Si non, me proposer une amélioration (ex : gestion des erreurs).
```

---

## **🚀 Conclusion**
En suivant ce guide, vous obtiendrez des réponses **rapides, précises et adaptées** à vos besoins. N'hésitez pas à me demander de l'aide si vous avez des doutes sur la façon de formuler votre prompt !

**À vous de jouer !** 🚀
