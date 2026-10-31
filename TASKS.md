# **TTFL_Helper - Roadmap (Backlog)**
*Liste des tâches séquentielles pour le développement de l'application.*

---

## **📌 Légende des Statuts**
- ❌ **À faire** : Tâche non commencée.
- ⏳ **En cours** : Tâche en cours de développement.
- ✅ **Terminé** : Tâche validée et testée.
- 🚧 **Bloqué** : Tâche bloquée (préciser la raison).

---

## **📌 Étape 0 : Initialisation du Projet**
| ID       | Titre                          | Objectif                                                                 | Description                                                                                     | Statut      |
|----------|--------------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------|
| **TASK-00** | Initialiser le dépôt Git       | Repository Git local avec `PROJECT.md` et `.gitignore`.                 | Créer un dépôt Git et ajouter les fichiers de base.                                           | ✅ Terminé   |
| **TASK-01** | Configurer l'environnement Python | Fichier `requirements.txt` ou `pyproject.toml` avec les dépendances.      | Ajouter `nba_api`, `rich`, `pytest`, etc. et créer un environnement virtuel.                  | ✅ Terminé   |

---

## **📌 Étape 1 : Fondations Techniques**
| ID       | Titre                          | Objectif                                                                 | Description                                                                                     | Statut      |
|----------|--------------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------|
| **TASK-02** | Créer la structure des dossiers | Arborescence du projet prête à l'emploi.                                | Créer les dossiers `src/`, `data/`, `tests/`, etc. (voir `PROJECT.md`).                          | ✅ Terminé   |
| **TASK-03** | Implémenter `config.py`         | Centraliser les chemins et paramètres globaux.                          | Définir les variables comme `DATA_DIR`, `CACHE_DIR`, `DB_PATH`, etc.                          | ✅ Terminé   |
| **TASK-04** | Implémenter `logger.py`         | Système de logging fonctionnel.                                         | Configurer `logging` pour afficher les erreurs et infos dans un fichier ou la console.        | ✅ Terminé   |

---

## **📌 Étape 2 : Récupération des Données (API NBA)**
| ID       | Titre                          | Objectif                                                                 | Description                                                                                     | Statut      |
|----------|--------------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------|
| **TASK-05** | Étudier `nba_api`               | Comprendre les endpoints disponibles.                                    | Lister les fonctions utiles (ex : `leaguegamelog`, `playergamelog`).                         | ❌ À faire   |
| **TASK-06** | Créer `nba_client.py`           | Récupérer les données des joueurs et matchs.                             | Implémenter des fonctions pour :<br>- Lister les joueurs actifs.<br>- Récupérer les stats d'un joueur.<br>- Récupérer le calendrier NBA. | ❌ À faire   |
| **TASK-07** | Tester `nba_client.py`          | Vérifier que les données sont récupérées correctement.                  | Écrire des tests unitaires pour valider les appels à `nba_api`.                               | ❌ À faire   |
| **TASK-08** | Implémenter `cache.py`          | Mettre en cache les données pour éviter les appels répétés.              | Sauvegarder les données en JSON (ex : `data/cache/players.json`) et les recharger si nécessaire. | ❌ À faire   |

---

## **📌 Étape 3 : Modélisation des Données**
| ID       | Titre                          | Objectif                                                                 | Description                                                                                     | Statut      |
|----------|--------------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------|
| **TASK-09** | Créer `models/player.py`        | Modèle `Player` avec ses attributs et méthodes.                          | Définir une classe `Player` avec :<br>- `id`, `name`, `team`<br>- `stats` (dictionnaire de moyennes)<br>- `last_selected_date` (pour le cooldown)<br>- Méthode `is_available(today)` pour vérifier le cooldown. | ❌ À faire   |
| **TASK-10** | Créer `models/team.py`          | Modèle `Team` (optionnel).                                               | Si besoin, modéliser les équipes pour filtrer les joueurs par équipe.                        | ❌ À faire   |
| **TASK-11** | Tester les modèles              | Valider les classes `Player` et `Team`.                                  | Écrire des tests pour les méthodes des modèles (ex : `is_available`).                         | ❌ À faire   |

---

## **📌 Étape 4 : Logique Métier (Cœur de l'Application)**
| ID       | Titre                          | Objectif                                                                 | Description                                                                                     | Statut      |
|----------|--------------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------|
| **TASK-12** | Implémenter `scorer.py`         | Calculer les points TTFL pour un joueur.                                | Définir la formule de calcul des points (ex : `PTS + REB + AST + STL + BLK - TO`).            | ❌ À faire   |
| **TASK-13** | Implémenter les moyennes glissantes | Calculer les moyennes sur N matchs pour chaque joueur.               | Ajouter une méthode dans `Player` ou `scorer.py` pour calculer la moyenne mobile.             | ❌ À faire   |
| **TASK-14** | Implémenter `selector.py`        | Algorithme de sélection du meilleur joueur disponible.                 | Créer une fonction `select_best_player(today, players)` qui :<br>- Filtre les joueurs disponibles (cooldown + match aujourd'hui).<br>- Trie par moyenne glissante ou score TTFL.<br>- Retourne le meilleur choix. | ❌ À faire   |
| **TASK-15** | Tester la logique métier        | Valider `scorer.py` et `selector.py`.                                    | Écrire des tests pour les fonctions de calcul et de sélection.                                | ❌ À faire   |

---

## **📌 Étape 5 : Base de Données (Historique)**
| ID       | Titre                          | Objectif                                                                 | Description                                                                                     | Statut      |
|----------|--------------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------|
| **TASK-16** | Initialiser la base SQLite     | Créer la base de données et les tables.                                  | Script `scripts/setup_db.py` pour créer :<br>- Table `players` (id, name, team, etc.).<br>- Table `selections` (player_id, date, score). | ❌ À faire   |
| **TASK-17** | Intégrer la DB dans `selector.py` | Sauvegarder les sélections historiques.                              | Mettre à jour la DB quand un joueur est sélectionné.                                           | ❌ À faire   |

---

## **📌 Étape 6 : Interface CLI**
| ID       | Titre                          | Objectif                                                                 | Description                                                                                     | Statut      |
|----------|--------------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------|
| **TASK-18** | Créer `cli/main.py`            | Point d'entrée de la CLI avec `click` ou `argparse`.                     | Implémenter des commandes comme :<br>- `ttfl update` (mettre à jour les données).<br>- `ttfl recommend` (recommander un joueur pour aujourd'hui).<br>- `ttfl history` (afficher l'historique des sélections). | ❌ À faire   |
| **TASK-19** | Intégrer `rich` pour l'affichage | Améliorer l'affichage CLI avec des tables et couleurs.                   | Utiliser `rich` pour afficher :<br>- Les joueurs disponibles.<br>- Les stats sous forme de tableau. | ❌ À faire   |
| **TASK-20** | Tester la CLI                   | Valider que toutes les commandes fonctionnent.                         | Tester manuellement ou avec des scripts.                                                       | ❌ À faire   |

---

## **📌 Étape 7 : Optimisations et Tests Finaux**
| ID       | Titre                          | Objectif                                                                 | Description                                                                                     | Statut      |
|----------|--------------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------|
| **TASK-21** | Optimiser les performances      | Réduire le temps de calcul pour les recommandations.                     | Utiliser le cache, éviter les appels redondants à `nba_api`, etc.                              | ❌ À faire   |
| **TASK-22** | Ajouter des tests d'intégration | Valider l'ensemble du workflow.                                           | Tester un scénario complet : mise à jour des données → sélection → sauvegarde en DB.          | ❌ À faire   |
| **TASK-23** | Documenter le code              | Ajouter des docstrings et commentaires.                                  | Documenter les fonctions et classes avec des exemples d'utilisation.                           | ❌ À faire   |

---

## **📌 Étape 8 : Déploiement et Maintenance**
| ID       | Titre                          | Objectif                                                                 | Description                                                                                     | Statut      |
|----------|--------------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------|
| **TASK-24** | Créer un script de déploiement  | Automatiser la mise en production.                                       | Script pour installer les dépendances et lancer l'application.                              | ❌ À faire   |
| **TASK-25** | Mettre à jour `PROJECT.md`      | Documenter l'état final du projet.                                       | Remplir la section "État d'Avancement" et ajouter des captures d'écran si possible.          | ❌ À faire   |

---

## **📌 Conseils pour Utiliser ce Backlog**
1. **Travailler par étapes** : Commencez par l'**Étape 0**, puis passez à l'**Étape 1**, etc.
2. **Valider chaque tâche** : Cochez ✅ une tâche uniquement quand elle est **testée et fonctionnelle**.
3. **Mettre à jour les statuts** : Modifiez ce fichier (`TASKS.md`) après chaque tâche terminée.
4. **Prioriser les blocages** : Si une tâche est bloquée (🚧), notez la raison et passez à une autre tâche si possible.
5. **Commits Git** : Faites un commit après chaque tâche validée pour suivre les progrès.

---

## **📌 Exemple de Workflow**
1. **Lire `PROJECT.md`** pour comprendre le contexte et les règles.
2. **Choisir une tâche** dans `TASKS.md` (ex : `TASK-02`).
3. **Développer** la fonctionnalité correspondante.
4. **Tester** la tâche (manuellement ou avec `pytest`).
5. **Mettre à jour `TASKS.md`** en cochant ✅ la tâche.
6. **Passer à la tâche suivante**.

---

## **📌 Notes Supplémentaires**
- **Dépendances entre tâches** : Certaines tâches dépendent d'autres (ex : `TASK-06` dépend de `TASK-05`). Respectez l'ordre.
- **Tests** : Les tâches de test (ex : `TASK-07`, `TASK-11`) doivent être faites **immédiatement après** la tâche de développement correspondante.
- **Documentation** : Mettez à jour `PROJECT.md` et `TASKS.md` régulièrement pour refléter l'état réel du projet.
