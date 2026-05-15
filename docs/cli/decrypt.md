# Commande `decrypt`

La commande `decrypt` permet de déchiffrer une archive `.oxz` chiffrée afin de la restaurer dans son format standard.

**Exemple d'utilisation :**
```bash
oxide decrypt archive_chiffree.oxz
```

## Arguments et Options

| Option / Argument | Description | Valeur par défaut |
| --- | --- | --- |
| `<input>` | L'archive `.oxz` source à déchiffrer. (Requis) | *Aucun* |
| `-o, --output` | Le chemin de destination. | Remplace le fichier original en toute sécurité |