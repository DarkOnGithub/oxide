# Commande `extract`

Cette commande permet de restaurer le contenu d'une archive `.oxz` vers un fichier ou un répertoire avec une vitesse de décodage hors du commun.

**Exemple d'utilisation :**
```bash
oxide extract archive.oxz
```

## Arguments et Options

Voici le résumé des paramètres pour `extract` :

| Option / Argument | Description | Valeur par défaut |
| --- | --- | --- |
| `<input>` | L'archive `.oxz` source à extraire. (Requis) | *Aucune* |
| `-o, --output` | Le chemin de destination complet. | `<input>.out` ou basé sur archive |
| `--only` | Permet de restaurer uniquement un fichier ou sous-dossier spécifique de l'archive. | *Aucun filtre* |
| `--only-regex` | Restaure les chemins relatifs correspondant à l'expression régulière donnée. | *Aucun filtre* |
| `--preset` | Nom du preset pour affiner le décodage. | *Géré par défaut* |
| `--preset-file` | Fichier JSON de préréglages (presets) personnalisé. | *Fichier intégré de l'outil* |
| `--workers` | Nombre de threads de décodage. `0` = auto (nb de cœurs). | *Paramètre du preset* |
| `--extract-write-shards` | Nombre de partitions d'écriture de répertoires (`0` = auto, `1` = sans partition). | `0` |
| `--stats-interval-ms` | Intervalle en ms pour le rafraîchissement des statistiques de progression. | `250` ms |
| `--telemetry-details` | Affiche les détails d'exécution complets en fin d'opération. | `false` |
