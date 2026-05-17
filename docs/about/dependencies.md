---
title: Dépendances
description: Bibliothèques et briques open-source utilisées dans l'écosystème Oxide
---

# Dépendances

Oxide repose sur plusieurs bibliothèques Rust et Web réparties entre le moteur, la CLI, la GUI et le site de documentation.

## Cœur et Compression (`oxide-core`)

La crate `oxide-core` concentre le format d'archive, la compression, l'extraction, le chiffrement et la résilience.

### Concurrence et pipeline

- `crossbeam-channel` : communication entre les étapes du pipeline
- `crossbeam-deque` : structures utiles pour l'ordonnancement parallèle
- `num_cpus` : adaptation du nombre de workers à la machine

### Compression et traitement binaire

- `lz4_flex` : implémentation du mode `fast`
- `zstd` : compression équilibrée et support des dictionnaires
- `liblzma` : compression `lzma` pour le mode `ultra`
- `bytes` et `memchr` : manipulation rapide de buffers binaires
- `regex` : filtres et sélections textuelles côté moteur

### Sécurité et intégrité

- `argon2` : dérivation de clé à partir du mot de passe
- `aes-gcm` : chiffrement authentifié par bloc
- `rand` : génération de sel et d'aléa cryptographique
- `crc32c` : vérification locale des blocs
- `blake3` : hachage rapide
- `reed-solomon-erasure` : génération et réparation des blocs de redondance

### Entrées / sorties et système

- `memmap2` : lecture mémoire mappée pour certains gros fichiers
- `jwalk` : parcours rapide des arborescences
- `libc` : accès système bas niveau lorsque nécessaire

### Sérialisation, erreurs et observabilité

- `serde` : sérialisation des structures de configuration et de métadonnées
- `anyhow` et `thiserror` : gestion d'erreurs
- `tracing` : instrumentation et logs

## Interface Ligne de Commande (`oxide`)

La crate `oxide-cli` fournit le binaire `oxide` et les commandes utilisateur.

- `clap` : parsing et validation des commandes
- `dialoguer` : interactions terminales pour les saisies utilisateur
- `nu-ansi-term` : coloration et lisibilité des sorties console
- `terminal_size` : adaptation de l'affichage à la largeur du terminal
- `serde` et `serde_json` : export JSON et lecture de presets
- `tracing-subscriber` : affichage et filtrage des logs

## Interface Graphique (`oxide-gui`)

La crate `oxide-gui` embarque une application native dédiée aux usages non CLI.

- `eframe` : couche d'application native
- `egui` : framework GUI en mode immédiat (via `eframe`)
- `rfd` : boîtes de dialogue natives pour les fichiers et dossiers

## Outils de test et développement

- `tempfile` : création de fichiers temporaires dans les tests du moteur

## Site de Documentation (Web)

Le site `docs/` est généré comme site statique.

- `vitepress` : générateur de site statique et navigation documentaire
- `vue` : composants et thèmes utilisés dans les pages d'accueil et d'affichage

## Identité Visuelle

- L'esthétique de notre **logo** s'inspire du travail de [SAWARATSUKI / KawaiiLogos](https://github.com/SAWARATSUKI/KawaiiLogos).

## Remarque

Cette page synthétise les dépendances les plus structurantes du projet. Les fichiers `Cargo.toml` et `package.json` restent la source de vérité pour la liste exacte et versionnée.
