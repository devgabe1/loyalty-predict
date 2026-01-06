#%%
import pandas as pd

import matplotlib.pyplot as plt

from sklearn import model_selection

import sqlalchemy

from feature_engine import selection
from feature_engine import imputation
from feature_engine import encoding

import mlflow

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment(experiment_id=1)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)



con = sqlalchemy.create_engine("sqlite:///../../data/analytics/database.db")

#%%

# SAMPLE - IMPORT DOS DADOS

df = pd.read_sql("SELECT * FROM abt_fiel", con)
df.head() 

#%%
# SAMPLE - OOT

df_oot = df[df['dtRef']==df['dtRef'].max()].reset_index(drop=True)
df_oot

#%%

# SAMPLE - Teste e Treino

target = 'flFiel'
features = df.columns.tolist()[3:]

df_train_test = df[df['dtRef']<df['dtRef'].max()].reset_index(drop=True)

y = df_train_test[target]   # Isso é um pd.Series (vetor)
X = df_train_test[features] # Isso é um pd.DataFrame (matriz)

X_train, X_test, y_train, y_test = model_selection.train_test_split(
    X, y,
    test_size=0.2,
    random_state=42,
    stratify=y,
)

print(f"Base Treino: {y_train.shape[0]} Unid. | Tx. Target {100*y_train.mean():.2f}%")
print(f"Base Test: {y_test.shape[0]} Unid. | Tx. Target {100*y_test.mean():.2f}%")


#%%

# EXPLORE - MISSING

s_nas = X_train.isna().mean()
s_nas = s_nas[s_nas > 0]
 
s_nas

#%%

## EXPLORE BIVARIADA

cat_features = ['descLifeCycleAtual', 'descLifeCycleD28']
num_features = list(set(features) - set(cat_features))
   
df_train = X_train.copy()
df_train[target] = y_train.copy()

df_train[num_features] = df_train[num_features].astype(float)

bivariada = df_train.groupby(target)[num_features].median().T
bivariada['ratio'] = (bivariada[1] + 0.001) / (bivariada[0]+0.001)
bivariada = bivariada.sort_values(by='ratio', ascending=False)
bivariada

# for i in to_remove:
#     features.remove(i)
#     num_features.remove(i)

# bivariada = df_train.groupby(target)[num_features].mean().T
# bivariada['ratio'] = (bivariada[1] + 0.001) / (bivariada[0]+0.001)
# bivariada.sort_values(by='ratio', ascending=False)

#%%

df_train.groupby('descLifeCycleAtual')[target].mean()

#%%
df_train.groupby('descLifeCycleD28')[target].mean()

# %%

# MODIFY - DROP

X_train[num_features] = X_train[num_features].astype(float)

to_remove = bivariada[bivariada['ratio']==1].index.tolist()

drop_features = selection.DropFeatures(to_remove)

#%%
# MODIFY - MISSING

fill_0 = ['python2025']
imput_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0, variables=fill_0)

imput_new = imputation.CategoricalImputer(
    fill_value='Nao-Usuario',
    variables=['descLifeCycleD28']
)

imput_1000 = imputation.ArbitraryNumberImputer(
    arbitrary_number=1000,
    variables=['avgIntervaloDiasVida',
               'avgIntervaloDiasD28',
               'qtdDiasUltimaAtividade']
)

# %% 
# MODIFY - ONEHOT

onehot = encoding.OneHotEncoder(variables=cat_features)

# %%
# MODEL

from sklearn import tree
from sklearn import ensemble

model = ensemble.RandomForestClassifier(
    random_state=42,
    n_estimators=100,
    min_samples_leaf=50,
)

# %%
# CRIANDO PIPELINE

from sklearn import pipeline

with mlflow.start_run() as r:

    model_pipeline = pipeline.Pipeline(steps=[
        ('Remoçao de Features', drop_features),
        ('Imputação de Zeros', imput_0),
        ('Imputação de Não-Usuario', imput_new),
        ('Imputação de 1000', imput_1000),
        ('OneHot Enconding', onehot),
        ('Algoritmo', model),
    ])

    model_pipeline.fit(X_train, y_train)

    # ASSESS - metricas
    from sklearn import metrics

    y_pred_train = model_pipeline.predict(X_train)
    y_proba_train = model_pipeline.predict_proba(X_train)

    acc_train = metrics.accuracy_score(y_train, y_pred_train)
    auc_train = metrics.roc_auc_score(y_train, y_proba_train[:,1])

    print("Acuracia Treino: ", acc_train)
    print("AUC Treino: ", auc_train)

    y_pred_test = model_pipeline.predict(X_test)
    y_proba_test = model_pipeline.predict_proba(X_test)
    
    
    acc_test = metrics.accuracy_score(y_test, y_pred_test)
    auc_test = metrics.roc_auc_score(y_test, y_proba_test[:,1])

    print('Acurácia Teste: ', acc_test)
    print('AUC Teste', auc_test)

    X_oot = df_oot[features]
    y_oot = df_oot[target]

    y_pred_oot = model_pipeline.predict(X_oot)
    y_proba_oot = model_pipeline.predict_proba(X_oot)

    acc_oot = metrics.accuracy_score(y_oot, y_pred_oot)
    auc_oot = metrics.roc_auc_score(y_oot, y_proba_oot[:,1])

    print('Acuracia OOT: ', acc_oot)
    print('AUC OOT: ', auc_oot)

    mlflow.log_metrics({
        'acc_train': acc_train,
        'auc_train': auc_train,
        'acc_test': acc_test,
        'auc_test': auc_test,
        'acc_oot': acc_oot,
        'auc_oot': auc_oot,

    })

    roc_train = metrics.roc_curve(y_train, y_proba_train[:,1])
    roc_test = metrics.roc_curve(y_test, y_proba_test[:,1])
    roc_oot = metrics.roc_curve(y_oot, y_proba_oot[:,1])

    plt.figure(dpi=200)
    plt.plot(roc_train[0], roc_train[1])
    plt.plot(roc_test[0], roc_test[1])
    plt.plot(roc_oot[0], roc_oot[1])
    plt.legend([f'Treino: {auc_train:.4f}', 
                f'Teste: {auc_test:.4f}',
                f'OOT: {auc_oot:.4f}'])
    plt.grid(True)
    plt.title('Curva ROC')
    plt.savefig('curva_roc.png')

    mlflow.log_artifact('curva_roc.png')
# %%
