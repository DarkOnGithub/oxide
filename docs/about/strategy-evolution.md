---
title: Évolution de la stratégie
description: Problématiques du pré-traitement et solutions adoptées
---

# Évolution de la stratégie : Prétraitement

Au cours du développement, plusieurs problématiques ont émergé concernant l'utilisation systématique d'algorithmes de pré-traitement avant la compression finale :

- **Surcoût de performance (Overhead)** : L'application successive de filtres de pré-traitement consomme d'importantes ressources et ralentit significativement la chaîne d'archivage, rendant l'opération trop coûteuse en temps d'exécution.
- **Rendement faible voire négatif** : De nombreux pré-traitements offrent une valeur ajoutée minime sur le taux de compression final de l'archive. Dans le pire des cas, la transformation finit même par **augmenter le volume** des données traitées.
- **Formats nativement compressés** : Plusieurs types de médias (tels que les formats d'images classiques ou l'audio) embarquent déjà des algorithmes de compression destructifs ou entropiques extrêmement efficaces. L'application d'un pré-traitement à l'aveugle sur ces fichiers n'apporte aucune aide et rajoute simplement de la latence.

## Solution apportée

Face à ces constats, nous avons pris la décision de **retirer cette étape de pré-traitement**. Cette coupure franche de la stratégie initiale permet :

1. D'**améliorer drastiquement les performances globales**, tant sur la vitesse brute de compression et décompression, que sur l'utilisation mémoire.
2. De libérer du temps et des ressources de développement afin de **nous concentrer sur l'implémentation d'autres fonctionnalités**. 
