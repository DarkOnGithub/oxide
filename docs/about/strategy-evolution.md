---
title: Évolution de la stratégie
description: Arbitrages techniques et changement de priorités au fil du projet
---

# Évolution de la stratégie

Oxide n'a pas suivi une trajectoire figée. Le projet a commencé avec une ambition large autour de la « compression universelle » et de multiples pré-traitements spécialisés, puis a évolué vers une plateforme d'archivage plus complète, plus cohérente et plus démontrable.

## Stratégie initiale

Au départ, l'objectif était de multiplier les transformations amont selon le type de contenu :

- pré-traitements image,
- pré-traitements texte,
- transformations binaires,
- traitements audio spécifiques.

Cette approche avait du sens sur le papier : mieux adapter les données à la compression finale. En pratique, elle a vite montré ses limites.

## Limites rencontrées

### 1. Un coût CPU trop élevé

Empiler des filtres avant la compression principale augmente fortement le temps total de traitement. Sur de gros volumes, l'overhead devenait trop visible face au gain réellement obtenu.

### 2. Un rendement souvent décevant

Plusieurs pré-traitements donnaient un bénéfice faible, nul, voire négatif sur la taille finale. Certains jeux de données compressaient déjà très bien sans étape supplémentaire.

### 3. Des formats déjà denses

Les images modernes, les archives existantes, les flux chiffrés ou certains médias compressés présentent déjà une entropie élevée. Les retravailler systématiquement ajoutait surtout de la latence.

### 4. Une priorité produit plus large que la seule compression

Pour la soutenance, il ne suffisait plus d'avoir un moteur théorique intéressant. Il fallait aussi un outil crédible de bout en bout :

- un format d'archive robuste,
- une CLI utilisable,
- une GUI démontrable,
- du chiffrement,
- une réparation en cas de corruption,
- des benchmarks reproductibles.

## Arbitrages retenus

L'équipe a donc déplacé l'effort vers des axes plus structurants.

### Compression : moins expérimentale, plus opérationnelle

Le mode équilibré initial basé sur **LZ77 + Huffman** a été abandonné au profit de **Zstd**, plus mature et plus performant dans le cadre du projet. Le mode ultra a été consolidé autour de **LZMA**.

### Pipeline : priorité au débit réel

Le travail a porté sur l'ordonnancement, la stabilité mémoire et l'efficacité globale du flux :

- files bornées,
- backpressure,
- buffers réutilisables,
- réordonnancement d'écriture,
- lecture adaptée aux fichiers volumineux,
- stockage brut automatique pour les blocs incompressibles.

### Format `.oxz` : priorité à la robustesse

La structure de l'archive a été simplifiée pour mieux coller à une écriture séquentielle réelle : header compact, données, manifest, table de blocs compacte, footer finalisant les offsets.

### Sécurité et résilience : fonctionnalités à forte valeur

Le projet s'est enrichi de deux dimensions essentielles :

- **confidentialité** avec Argon2id + AES-256-GCM,
- **tolérance à la corruption** avec CRC32C, Reed-Solomon et réparation Out-of-Band.

### Interface : rendre le moteur accessible

La GUI est devenue un livrable majeur. Elle permet d'exposer les fonctionnalités du moteur à un utilisateur non technique, sans sacrifier la réactivité de l'application.

## Résultat de cette évolution

Cette réorientation a rendu Oxide plus cohérent comme produit technique :

- moins dépendant de pré-traitements fragiles,
- plus rapide sur des usages réels,
- plus robuste face aux corruptions,
- plus complet grâce au chiffrement et à la GUI,
- plus défendable grâce à des benchmarks comparatifs.

En résumé, la stratégie a évolué d'un prototype de compression multi-idées vers un **archiveur complet, mesuré et utilisable**.
