# Importe la classe MRJob depuis la bibliothèque mrjob pour la création de jobs MapReduce.
from mrjob.job import MRJob
# Importe le module os pour accéder aux fonctionnalités du système d'exploitation.
import os

# Définit la classe MRInvertedIndex qui hérite de MRJob pour créer un index inversé.
class MRInvertedIndex(MRJob):
    # La classe est conçue pour exécuter un travail MapReduce afin de générer un index inversé.

    def mapper(self, _, line):
        # La fonction mapper est appelée pour chaque ligne de texte dans le fichier d'entrée.
        words = line.split()  # Sépare la ligne en mots individuels.
        # Récupère le nom du fichier en cours de traitement comme identifiant du document.
        doc_id = os.path.basename(os.environ['map_input_file']).split('.')[0]
        
        # Pour chaque mot de la ligne, génère une paire clé-valeur.
        for word in words[0:]:
            yield (word, doc_id)  # Émet la paire composée du mot et de l'identifiant du document.

    def reducer(self, key, values):
        # La fonction reducer est appelée pour chaque clé unique générée par le mapper.
        # Compile une liste de tous les identifiants de documents associés à chaque mot (clé).
        doc_ids = list(values)
        yield (key, doc_ids)  # Émet la paire composée du mot et de la liste des identifiants de documents.

# Si le script est exécuté comme un programme principal, lance le job MapReduce.
if __name__ == "__main__":
    MRInvertedIndex.run()
