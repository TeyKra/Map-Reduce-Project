# Importe la classe MRJob pour créer des tâches MapReduce.
from mrjob.job import MRJob
# Importe le module os pour interagir avec le système d'exploitation.
import os

# Définit la classe MRDocumentList2times héritant de MRJob.
class MRDocumentList2times(MRJob):
    
    def mapper(self, _, line):
        # Sépare chaque ligne en mots.
        words = line.split()
        # Récupère le nom du fichier courant et l'utilise comme identifiant de document (ex : D1, D2...).
        doc_id = os.path.basename(os.environ['map_input_file']).split('.')[0]

        # Vérifie si la ligne contient au moins un mot.
        if len(words) >= 1:
            # Traite chaque mot de la ligne (jusqu'à deux mots).
            for word in words[:2]:
                # Pour chaque mot, émet deux paires clé-valeur : une pour le mot et le document,
                # et une autre pour le mot et un marqueur '*' utilisé pour le comptage.
                yield (word, doc_id)
                yield (word, '*')

    def reducer(self, key, values):
        # Convertit les valeurs en liste pour faciliter leur manipulation.
        value_list = list(values)
        # Compte le nombre de documents (marqueurs '*') pour chaque mot.
        num_docs = value_list.count('*')

        # Si un mot apparaît dans au moins deux documents différents,
        # émet chaque document associé à ce mot.
        if num_docs >= 2:
            for v in value_list:
                if v != '*':
                    yield (v, key)

# Point d'entrée principal pour exécuter la tâche MapReduce.
if __name__ == "__main__":
    MRDocumentList2times.run()
