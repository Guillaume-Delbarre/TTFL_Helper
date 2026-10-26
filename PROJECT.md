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
- **Points TTFL** : Les points sont calculés à partir des **statistiques réelles** du joueur en match avec la formule suivante :
  **`POINTS + REBONDS + PASSES + INTERCEPTIONS + CONTRES + TIRS RÉUSSIS + 3PTS RÉUSSIS + LF RÉUSSIS - (BALLES PERDUES + TIRS RATÉS + 3PTS RATÉS + LF RATÉS)`**.
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

