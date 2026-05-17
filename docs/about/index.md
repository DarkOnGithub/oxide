---
title: À propos
description: Présentation du projet, de l'équipe et des livrables d'Oxide
---

# À propos d'Oxide

Oxide est un archiveur Rust conçu autour d'un format natif `.oxz`, d'un pipeline parallèle par blocs et d'un outillage complet pour l'archivage, l'extraction, le chiffrement et la réparation d'archives. Le projet est développé dans le cadre du Projet S4 (2026).

## Ce que couvre le projet

- **Archivage et extraction** de fichiers uniques ou d'arborescences complètes
- **Compression multi-profils** avec `lz4`, `zstd` et `lzma`
- **Sécurité** avec dérivation de clé `Argon2id` et chiffrement authentifié `AES-256-GCM`
- **Résilience** avec vérification locale, protection Reed-Solomon et réparation d'archives
- **Interfaces utilisateur** via une CLI Rust et une GUI native
- **Mesure des performances** via benchmarks, télémétrie et profils de presets

## Répartition de l'équipe

### Alexandre Joaquim Lima Salgueiro <a href="https://github.com/Niponx" target="_blank" title="GitHub"><img src="https://cdn.simpleicons.org/github/white" width="24" height="24" style="display:inline-block; vertical-align:text-bottom; margin-left:6px" alt="GitHub"/></a>

**Responsabilité principale : sécurité et architecture des commandes de résilience**

- Implémentation de la dérivation de clé **Argon2id**
- Intégration du chiffrement authentifié **AES-256-GCM** par bloc
- Conception des commandes CLI **`protect`** et **`repair`**
- Définition de la structure des métadonnées de récupération
- Contribution au sous-système de protection contre la corruption

### William Huang Hong <a href="https://github.com/CHALUTe" target="_blank" title="GitHub"><img src="https://cdn.simpleicons.org/github/white" width="24" height="24" style="display:inline-block; vertical-align:text-bottom; margin-left:6px" alt="GitHub"/></a>

**Responsabilité principale : robustesse, détection et moteur mathématique de réparation**

- Conception de l'architecture de réparation **Out-of-Band (V2)**
- Implémentation du moteur **Reed-Solomon**
- Intégration de la détection d'altération par **CRC32C**
- Contribution à la CLI et aux flux de restauration d'archives

### Romain Bailly <a href="https://github.com/linkito94" target="_blank" title="GitHub"><img src="https://cdn.simpleicons.org/github/white" width="24" height="24" style="display:inline-block; vertical-align:text-bottom; margin-left:6px" alt="GitHub"/></a>

**Responsabilité principale : interface graphique et communication technique**

- Développement de la GUI native **Oxide Toolkit** en Rust
- Intégration des **7 modes opératoires** dans une interface unifiée
- Ajout des options de chiffrement et de protection directement au moment de la compression
- Stabilisation de l'application sous Linux/Wayland et amélioration des mises à jour de progression
- Conception et maintenance du **site de documentation**

### Tom Huynh <a href="https://github.com/DarkOnGithub" target="_blank" title="GitHub"><img src="https://cdn.simpleicons.org/github/white" width="24" height="24" style="display:inline-block; vertical-align:text-bottom; margin-left:6px" alt="GitHub"/></a><a href="https://x.com/__Dark______" target="_blank" title="X (Twitter)"><img src="https://cdn.simpleicons.org/x/white" width="22" height="22" style="display:inline-block; vertical-align:text-bottom; margin-left:8px" alt="X"/></a>

**Responsabilité principale : moteur de compression, pipeline et format d'archive**

- Intégration de **LZ4** pour le profil `fast`
- Intégration de **Zstandard (Zstd)** pour le profil `balanced`
- Intégration de **LZMA** pour le profil `ultra`
- Évolution du format **`.oxz`** avec header compact, table de blocs compacte et footer de résolution
- Optimisations de pipeline : backpressure, buffers réutilisables, écriture ordonnée, stockage brut automatique, détection d'incompressibilité
- Benchmarks comparatifs face à d'autres outils

## État d'avancement – deuxième soutenance

| Domaine | État actuel |
| --- | --- |
| Compression | `lz4`, `zstd` et `lzma` intégrés et reliés aux presets `fast`, `balanced`, `ultra` |
| Format `.oxz` | structure V3 stabilisée autour d'un header compact, d'un manifeste, d'une table de blocs et d'un footer |
| Sécurité | chiffrement par mot de passe opérationnel avec validation et déchiffrement symétrique |
| Résilience | détection locale des corruptions, protection Reed-Solomon et réparation intégrées |
| CLI | commandes d'archivage, extraction, visualisation, chiffrement et réparation disponibles |
| GUI | application native exécutable, non bloquante, avec progression et protections UX |
| Benchmarks | campagne comparative réalisée sur un jeu de données mixte de 5.7 GB |

## Ce qui a changé depuis la première phase

- Le mode équilibré prévu autour de **LZ77 + Huffman** a été remplacé par **Zstd**
- Le mode ultra repose désormais sur **LZMA**
- L'effort s'est déplacé des pré-traitements expérimentaux vers des fonctions plus directement utiles : **format, sécurité, robustesse, GUI et performance mesurée**
- Le site de documentation sert maintenant aussi de vitrine technique pour la soutenance


