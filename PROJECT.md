# **TTFL_Helper**
*Optimiseur de choix de joueurs pour la Trashtalk Fantasy League (TTFL)*

---

## 📌 **Vision et Objectifs**
### **Pourquoi ce projet ?**
La **Trashtalk Fantasy League (TTFL)** est un jeu de fantasy basketball où chaque jour, un joueur est choisi pour marquer des points basés sur ses statistiques réelles en match NBA. La **contrainte majeure** est qu'un joueur sélectionné devient **indisponible pendant 30 jours** après sa sélection. L'objectif est de maximiser les points cumulés sur une saison en optimisant les choix de joueurs, en tenant compte :
- Des **performances récentes** (moyennes glissantes).
- Des **indisponibilités** (30 jours de cooldown).
- Des **blessures** (joueurs inactifs).
- Des **affinités** (ex : joueurs avec des stats élevées en rebonds ou en passes).

**TTFL_Helper** est une **CLI Python** conçue pour :
✅ **Automatiser** la récupération des données NBA (via `nba_api`).
✅ **Simuler** des stratégies de sélection optimales.
✅ **Recommander** le meilleur joueur disponible chaque jour.
✅ **Visualiser** les performances et les tendances (via `rich` pour un affichage CLI avancé).

---

## 🏀 **Règles Métier**
### **1. Contraintes du jeu TTFL**
- **1 joueur par jour** : Chaque jour, un seul joueur peut être sélectionné pour marquer des points.
- **Cooldown de 30 jours** : Un joueur sélectionné ne peut **pas** être choisi à nouveau pendant **30 jours** (soit ~30 matchs).
- **Points TTFL** : Les points sont calculés à partir des **statistiques réelles** du joueur en match (ex : 1 point = 1 point NBA, 1 rebond = 1 point, 1 passe = 1 point, etc.).
  *À préciser* : La formule exacte de calcul des points TTFL doit être confirmée (ex : `points = PTS + REB + AST + STL + BLK - TO`).

### **2. Calcul des moyennes**
- **Moyenne glissante** : Pour chaque joueur, calculer une moyenne mobile sur les **5 derniers matchs** (ou un nombre configurable) pour évaluer sa forme actuelle.
- **Moyenne saisonnière** : Moyenne des stats sur toute la saison pour identifier les joueurs constants.
- **Poids des moyennes** : Prioriser les joueurs avec une **moyenne glissante élevée** et une **tendance à la hausse**.

### **3. Critères de sélection**
Un joueur est éligible si :
- Il n'a **pas été sélectionné dans les 30 derniers jours**.
- Il est **actif** (pas blessé, pas suspendu).
- Il a un **match prévu le jour J** (vérification via le calendrier NBA).

---

## 🛠 **Stack Technique**
| Composant          | Technologie                     | Justification                                                                 |
|--------------------|----------------------------------|-------------------------------------------------------------------------------|
| **Langage**        | Python 3.10+                     | Syntaxe moderne, typage fort (`typing`), support des `dataclasses`.          |
| **Récupération des données** | [`nba_api`](https://github.com/swar/nba_api) | Bibliothèque officielle pour accéder aux stats NBA (matchs, joueurs, etc.). |
| **CLI**            | `click` ou `argparse`             | Pour une interface en ligne de commande intuitive.                         |
| **Affichage CLI**  | [`rich`](https://github.com/Textualize/rich) | Tables, graphiques et couleurs dans le terminal.                            |
| **Stockage**       | JSON (fichiers) + SQLite         | - JSON : Cache des données brutes (ex : `data/players.json`).<br>- SQLite : Stockage des sélections historiques et des métadonnées. |
| **Tests**          | `pytest`                         | Tests unitaires pour les fonctions critiques (ex : calcul des moyennes).    |
| **Gestion des dépendances** | `poetry` ou `pip` + `requirements.txt` | Pour un environnement reproductible.                                      |
| **Logging**        | `logging` (module standard)      | Suivi des erreurs et des actions (ex : récupération des données).           |

---

## 📊 **État d'Avancement**
| Étape               | Statut          | Date de mise à jour | Commentaires                     |
|---------------------|-----------------|--------------------|-----------------------------------|
| **Fondations**      | ✅ **Terminé**   | [Date]             | Structure du projet et `PROJECT.md`. |
| **Récupération des données** | ⏳ **En cours** | -                  | Intégration de `nba_api`.          |
| **Calcul des moyennes** | ❌ **À faire**  | -                  | Implémentation des formules.      |
| **Système de recommandation** | ❌ **À faire** | -                  | Algorithme de sélection optimale. |
| **CLI**             | ❌ **À faire**   | -                  | Interface utilisateur.             |
| **Tests**           | ❌ **À faire**   | -                  | Couverture des fonctions clés.     |

*Mise à jour : [Date]*

---

## 📁 **Structure du Projet**
```
ttfl_helper/
│
├── PROJECT.md               # Fichier de référence
├── requirements.txt         # Dépendances Python
├── pyproject.toml           # Configuration (optionnel, pour `poetry`)
├── .gitignore               # Fichiers à exclure de Git
│
├── data/                    # Données stockées localement
│   ├── cache/               # Cache des données brutes (JSON)
│   │   ├── players.json
│   │   ├── games.json
│   │   └── ...
│   └── db/                  # Base de données SQLite
│       └── ttfl.db
│
├── src/                     # Code source
│   ├── __init__.py
│   │
│   ├── api/                 # Module pour la récupération des données
│   │   ├── __init__.py
│   │   └── nba_client.py    # Client pour `nba_api`
│   │
│   ├── core/                # Logique métier
│   │   ├── __init__.py
│   │   ├── selector.py       # Algorithme de sélection des joueurs
│   │   └── scorer.py         # Calcul des points TTFL
│   │
│   ├── models/              # Modèles de données
│   │   ├── __init__.py
│   │   ├── player.py         # Classe `Player`
│   │   └── team.py           # Classe `Team`
│   │
│   ├── utils/               # Utilitaires
│   │   ├── __init__.py
│   │   ├── cache.py          # Gestion du cache (JSON)
│   │   ├── config.py         # Configuration (chemins, paramètres)
│   │   └── logger.py         # Logging
│   │
│   └── cli/                 # Interface en ligne de commande
│       ├── __init__.py
│       └── main.py           # Point d'entrée de la CLI
│
├── tests/                   # Tests unitaires
│   ├── __init__.py
│   ├── test_api.py          # Tests pour `nba_client.py`
│   ├── test_core.py         # Tests pour `selector.py` et `scorer.py`
│   └── test_models.py       # Tests pour les modèles
│
└── scripts/                 # Scripts utilitaires (optionnel)
    └── setup_db.py          # Initialisation de la base de données
```

---

## 🎯 **Roadmap (Backlog)**
Voir le fichier `TASKS.md` à la racine du projet pour la liste détaillée des tâches.
