# Importe la classe MRJob depuis la bibliothèque mrjob, utilisée pour créer des jobs MapReduce.
from mrjob.job import MRJob
# Importe le module re pour les opérations d'expressions régulières.
import re

# Compile une expression régulière pour identifier les mots dans un texte. 
# Cette expression reconnaît les mots constitués de caractères alphanumériques et d'apostrophes.
WORD_RE = re.compile(r"[\w']+")

# Définit une classe MRWordFrequencyCount héritant de MRJob pour le traitement MapReduce.
class MRWordFrequencyCount(MRJob):

    # Définit la fonction 'mapper' qui est appelée pour chaque ligne du fichier d'entrée.
    def mapper(self, _, line):
        # Utilise l'expression régulière compilée pour trouver tous les mots dans la ligne.
        for word in WORD_RE.findall(line):
            # Pour chaque mot trouvé, renvoie une paire clé-valeur (mot en minuscule, 1).
            yield (word.lower(), 1)

    # Définit la fonction 'reducer' qui est appelée pour chaque clé unique générée par le 'mapper'.
    def reducer(self, key, values):
        # Pour chaque mot (clé), calcule la somme des valeurs (fréquences) et renvoie la paire (mot, fréquence totale).
        yield (key, sum(values))

# Si le script est exécuté directement, lance le job MapReduce.
if __name__ == "__main__":
    MRWordFrequencyCount.run()
