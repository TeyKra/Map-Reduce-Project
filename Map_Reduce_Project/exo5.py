from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import csv

class MRSales(MRJob):
    # La classe hérite de MRJob pour utiliser les fonctionnalités de MapReduce.

    Output = TextProtocol  
    # Définit le format de sortie des résultats comme du texte brut.

    def configure_args(self):
        super(MRSales, self).configure_args()
        # Appelle la méthode parente pour configurer les arguments par défaut.

        # Ajoute des arguments personnalisés pour spécifier la tâche et le filtre des ventes.
        self.add_passthru_arg('-t', '--task', help='Tâche : region, country, item_type')
        # Ajoute un argument pour spécifier la tâche (ex: 'region', 'country', 'item_type').

        self.add_passthru_arg('--onoff', help='Préciser les ventes en ligne ou hors ligne', default=None)
        # Ajoute un argument pour filtrer les données par canal de vente (en ligne ou hors ligne).

    def mapper(self, _, line):
        # Définit la fonction de mappage qui traite chaque ligne du fichier d'entrée.

        if line.startswith('Region,Country,Item Type,Sales Channel'):
            return
        # Ignore la première ligne si c'est l'en-tête du fichier CSV.

        row = next(csv.reader([line]))
        # Convertit la ligne en une liste en utilisant le module csv.

        region, country, item_type, sales_channel, total_profit = row[0], row[1], row[2], row[3], float(row[-1])
        # Extrait les informations nécessaires de la ligne.

        if self.options.onoff and self.options.onoff.replace('-', '_').lower() != sales_channel.lower():
            return
        # Vérifie si la ligne correspond au filtre des ventes spécifié.

        if self.options.task == 'region':
            yield region, total_profit
        elif self.options.task == 'country':
            yield country, total_profit
        elif self.options.task == 'item_type':
            yield item_type, total_profit
        # Génère des paires clé-valeur pour le processus de réduction, selon la tâche spécifiée.

    def reducer(self, key, values):
        # Définit la fonction de réduction pour traiter chaque clé et ses valeurs associées.

        yield key, f"{sum(values):.2f}"
        # Calcule la somme des profits pour chaque clé et renvoie le résultat.

if __name__ == '__main__':
    MRSales.run()
    # Exécute le script en mode MapReduce si le script est exécuté directement.
