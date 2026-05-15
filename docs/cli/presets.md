# Configuration des Presets

Le système de *Presets* dans Oxide vous donne la possibilité d'ajuster finement tous les paramètres de compression, comme la quantité de RAM consommée ou les stratégies de *threading*, en passant un simple nom.

Par défaut, l'outil utilise **`balanced`** mais vous pouvez sélectionner d'autres configurations selon vos priorités en utilisant la commande d'archive via l'argument `--preset <nom>`.

```bash
oxide archive mondossier/ --preset ultra
```

## Presets Inclus

Le fichier par défaut de `oxide` contient 3 profils distincts aux paramètres préréglés :

### `fast` (Compression ultra-rapide)
Pensé pour exploiter le maximum du disque dur sans monopoliser le CPU. Idéal pour des sauvegardes massives de données déjà denses.
- **Compression** : `lz4`
- **Block Size** : `3M`
- **In-flight Bytes** : `2G`
- **Workers** : Automatique (0)
- **Producer Threads** : `4`
- **Chunking** : `fixed`

### `balanced` (Par défaut)
S'adapte parfaitement à un usage général. Exploite `zstd` pour garantir un excellent rapport volume de stockage / vitesse d'exécution.
- **Compression** : `zstd` (Niveau 2)
- **Block Size** : `2M`
- **In-flight Bytes** : `2G`
- **Workers** : Automatique (0)
- **Chunking** : `fixed` (`512K` à `4M` possibles en mode CDC)
- **Producer Threads** : `3`

### `ultra` (Le format de taille mini)
Idéal si vous prévoyez d'archiver vos serveurs et bases de données vers le Cloud ou pour un envoi longue distance. Moins rapide, mais génère la plus petite archive possible.
- **Compression** : `lzma` (Niveau 7, Dictionnaire `8M`)
- **Block Size** : `3M`
- **In-flight Bytes** : `768M`
- **Workers** : Automatique (0)
- **Chunking** : `fixed` (`768K` à `6M` possibles en mode CDC)
- **Producer Threads** : `3`

## Personnaliser les Profils

Si besoin, vous pouvez créer votre propre fichier de configuration JSON calqué sur la structure par défaut (`--preset-file mondossier/presets.json`) avec une section `"defaults"` (valeurs par défaut) et `"presets"`.
