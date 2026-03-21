# Configuration des Presets

Le système de *Presets* dans Oxide vous donne la possibilité d'ajuster finement tous les paramètres de compression, comme la quantité de RAM consommée ou les stratégies de *threading*, en passant un simple nom.

Par défaut, l'outil utilise **`balanced`** mais vous pouvez sélectionner d'autres configurations selon vos priorités en utilisant la commande d'archive via l'argument `--preset <nom>`.

```bash
oxide archive mondossier/ --preset ultra
```

## Presets Inclus

Le fichier par défaut de `oxide` contiens 3 profils distincts aux paramètres préréglés :

### `fast` (Compression ultra-rapide)
Pensé pour exploiter le maximum du disque dur sans monopoliser de CPU. Idéal pour des sauvegardes massives de données déjà denses.
- **Compression** : `lz4` avec preset `fast`
- **Block Size** : `1M`
- **In-flight Bytes** : `3G`
- **Workers** : `6`

### `balanced` (Par défaut)
S'adapte parfaitement à un usage de bureau. Exploite le `zstd` pour garantir un excellent rapport volume de stockage / vitesse d'exécution.
- **Compression** : `zstd` (Niveau 6) avec preset `balanced`
- **Block Size** : `2M`
- **In-flight Bytes** : `3G`
- **Workers** : `14` (Par défaut d'ensemble)

### `ultra` (Le format de taille mini)
Idéal si vous prévoyez d'archiver vos serveurs et bases de données vers le Cloud ou de l'envoi longue distance. Moins rapide, mais génère la plus petite archive.
- **Compression** : `zstd` (Niveau 19) avec preset `high`
- **Block Size** : `4M`
- **In-flight Bytes** : `4G`

## Personnaliser les Profils

Si besoin, vous pouvez créer votre propre fichier de configuration JSON calqué sur la structure par défaut (`--preset-file mondossier/presets.json`) avec une section `"defaults"` (valeurs mères) et `"presets"`.
