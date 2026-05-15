# Commande `archive`

La commande `archive` permet de compresser un fichier ou un répertoire entier vers une archive `.oxz` hautement optimisée.

**Exemple d'utilisation :**
```bash
oxide archive mondossier/
```

## Arguments et Options

Voici la liste des arguments supportés, avec leurs valeurs par défaut.

| Option / Argument | Description | Valeur par défaut |
| --- | --- | --- |
| `<input>` | Le fichier ou répertoire source à archiver. (Requis) | *Aucun* |
| `-o, --output` | Le chemin de l'archive de destination. | `<input>.oxz` |
| `--compression` | L'algorithme de compression à utiliser (`lz4`, `lzma`, `zstd`). | *Géré par les presets* |
| `--compression-level` | Le niveau de compression explicite (spécifique au codec). | *Géré par les presets* |
| `--skip-compression` | Ne faire aucune compression (mode stockage). | `false` |
| `--encrypt` | Chiffrer l'archive avec un mot de passe. | `false` |
| `--chunking` | Le mode de découpage des blocs (`fixed` ou `cdc`). | *Géré par les presets* |
| `--min-block-size` | La taille minimale des blocs en mode CDC. | *Géré par les presets* |
| `--max-block-size` | La taille maximale des blocs en mode CDC. | *Géré par les presets* |
| `--dictionary-from` | Réutiliser la banque de dictionnaire d'une archive existante. | *Aucune* |
| `--preset` | Choisir un préréglage d'archivage existant. | `balanced` |
| `--preset-file` | Fichier JSON de préréglages (presets) personnalisé. | *Fichier intégré de l'outil* |
| `--block-size` | La taille cible des blocs (ex: `64K`, `1M`). | *Géré par les presets* |
| `--workers` | Nombre de threads de compression. `0` = auto (nb de cœurs). | `0` (Automatique) |
| `--pool-capacity` | Capacité par défaut du pool de buffers (ex: `1M`). | *Géré par les presets* |
| `--pool-buffers` | Nombre maximum de buffers conservés par le pool. | *Géré par les presets* |
| `--stats-interval-ms` | Intervalle de rafraîchissement de la progression en ms. | *Géré par les presets* |
| `--inflight-bytes` | Octets de charge utile en cours de traitement (ex: `2G`). | *Géré par les presets* |
| `--inflight-blocks-per-worker` | Blocs maximum en cours de traitement par worker. | *Géré par les presets* |
| `--stream-read-buffer` | Taille du buffer de lecture pour l'entrée répertoire. | *Géré par les presets* |
| `--producer-threads` | Nombre total de threads producteurs. | *Géré par les presets* |
| `--directory-mmap-threshold` | Seuil de taille pour l'utilisation de `mmap` sur l'entrée répertoire. | *Géré par les presets* |
| `--writer-queue-blocks` | Capacité de la file d'attente des résultats d'écriture. | *Géré par les presets* |
| `--result-wait-ms` | Délai d'attente maximum des résultats des workers en ms. | *Géré par les presets* |
| `--telemetry-details` | Affiche un tableau détaillé des statistiques en fin d'exécution. | `false` |

*(Note : De nombreux paramètres avancés dépendent du preset configuré. Consultez la section sur [Les Presets](./presets.md) pour plus de détails.)*