# Rappel

L'utilisation de python natif est strictement interdit. Vous devez utiliser les environnements virtuelles gérés par uv.

Parfait, chef 😎 ! Voilà un petit exemple concret où on utilise Pyment pour générer des docstrings et Flake8 pour vérifier le style du code.

1️⃣ On part d’un fichier Python “exemple.py” sans docstrings :
def add(a,b):
    return a+b

def multiply(x,y):
    return x*y

2️⃣ On utilise Pyment pour générer des docstrings

Commande :

pyment -w exemple.py


Le -w dit : écris directement dans le fichier.

Après Pyment, le fichier devient :

def add(a, b):
    """
    TODO: Docstring for add.
    """
    return a + b


def multiply(x, y):
    """
    TODO: Docstring for multiply.
    """
    return x * y


Pyment a créé les docstrings “squelettes” qu’on peut ensuite compléter pour décrire les paramètres et retours.

3️⃣ On vérifie le style avec Flake8

Commande :

flake8 exemple.py


Sortie typique :

exemple.py:1:10: E231 missing whitespace after ','
exemple.py:2:5: WPS210 Found nested function definition


Ici :

E231 → espace manquant après la virgule → add(a,b) doit être add(a, b)

On peut corriger pour respecter PEP 8 :

def add(a, b):
    """
    Additionne deux nombres.
    
    Args:
        a (int): Premier nombre
        b (int): Deuxième nombre

    Returns:
        int: Somme de a et b
    """
    return a + b


def multiply(x, y):
    """
    Multiplie deux nombres.
    
    Args:
        x (int): Premier nombre
        y (int): Deuxième nombre

    Returns:
        int: Produit de x et y
    """
    return x * y


✅ Résultat final :

Docstrings claires pour la documentation → Pyment

Code propre et conforme au style Python → Flake8