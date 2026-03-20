# Commande `extract`

Cette commande permet de restaurer le contenu d'une archive `.oxz` vers un fichier ou un répertoire avec une vitesse de décodage hors du commun.

**Exemple d'utilisation :**
```bash
oxide-cli extract archive.oxz
```

## Arguments et Options

Voici le résumé des paramètres pour `extract` :

| Option / Argument | Description | Valeur par défaut |
| --- | --- | --- |
| `<input>` | L'archive `.oxz` source à extraire. (Requis) | *Aucune* |
| `-o, --output` | Le chemin de destination complet. | `<input>.out` ou basé sur archive |
| `--only` | Permet de restaurer uniquement un fichier ou sous-dossier spécifique de l'archive. | *Aucune filtre* |
| `--only-regex` | Restaure les chemins relatifs correspondant à l'expression régulière donnée. | *Aucun filtre* |
| `--workers` | Nombre de threads de décodage. | *Nombre de cœurs (`num_cpus`)* |
| `--stats-interval-ms` | Intervalle en ms pour le rafraîchissement des statistiques de progression. | `250` ms |
| `--telemetry-details` | Affiche les détails d'exécution complets en fin d'opération. | `false` |
