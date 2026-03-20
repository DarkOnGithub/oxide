# CLI Documentation

Bienvenue dans la documentation d'**Oxide** ! Oxide est un outil d'archivage haute performance développé en Rust.

## Installation

Vous pouvez installer `oxide-cli` soit en utilisant Cargo (le gestionnaire de paquets Rust), soit en téléchargeant directement un binaire pré-compilé depuis notre dépôt.

### Option 1 : Utilisation de Cargo

Si Rust et Cargo sont déjà installés sur votre système, vous pouvez compiler et installer Oxide directement :

```bash
cargo install oxide-cli
```

::: info Remarque
Assurez-vous que le dossier `~/.cargo/bin` figure bien dans votre variable d'environnement `PATH` pour pouvoir exécuter `oxide-cli` depuis n'importe où.
:::

### Option 2 : Téléchargement depuis le dépôt

Si Rust n'est pas installé ou si vous préférez un exécutable prêt à l'emploi, vous pouvez télécharger les derniers binaires pré-compilés depuis la page GitHub Releases :

1. Accédez aux [Versions (Releases) de GitHub Oxide](https://github.com/DarkOnGithub/oxide/releases/latest).
2. Téléchargez l'archive correspondant à votre système d'exploitation.
3. Extrayez l'archive téléchargée.
4. Placez l'exécutable dans un répertoire inclus dans votre `PATH`.

---

## Prochaines étapes

Une fois l'installation terminée, vérifiez qu'Oxide fonctionne correctement en affichant sa version :

```bash
oxide-cli --version
```

Pour plonger dans le vif du sujet et apprendre à utiliser les paramètres avancés d'Oxide, consultez notre page [Archive usage](./archive.md).
