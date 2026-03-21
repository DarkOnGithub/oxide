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
| `<input>` | Le fichier ou répertoire source à archiver. (Requis) | *Aucune* |
| `-o, --output` | Le chemin de l'archive de destination. | `<input>.oxz` |
| `--compression` | L'algorithme de compression à utiliser (`lz4` ou `zstd`). | *Géré par les presets* |
| `--zstd-level` | Le niveau de compression si l'algorithme `zstd` est choisi (entre 1 et 22). | *Géré par les presets* |
| `--preset` | Choisir un préréglage d'archivage existant. | `balanced` |
| `--preset-file` | Fichier JSON de préréglages (presets) personnalisé. | Fichier intégré de l'outil |
| `--block-size` | La taille cible des blocs (ex: `64K`, `1M`). | *Géré par les presets* |
| `--workers` | Nombre de threads de compression. `0` = auto (nb de cœurs). | `0` (Automatique) |
| `--skip-preprocessing` | Stocker les blocs sans prétraitement (preprocessing). | `false` |
| `--skip-compression` | Ne faire aucune compression (mode stockage). | `false` |
| `--telemetry-details` | Affiche un tableau détaillé des statistiques en fin d'exécution. | `false` |

*(Note : De nombreux paramètres avancés comme `pool-capacity`, `inflight-bytes`, etc., dépendent du preset configuré. Consultez la section sur [Les Presets](./presets.md) pour plus de détails.)*
