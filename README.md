# 📊 Painel de Tomada de Decisão com Dados do Compras.gov.br

### Projeto Multidisciplinar Integrador – Unicesusc

Este repositório reúne o código e os dados utilizados no desenvolvimento de um painel de apoio à tomada de decisão para gestores públicos, com foco na análise de **contratos governamentais publicados no portal Compras.gov.br**.

---

### Objetivo do Projeto

O projeto tem como finalidade contribuir com a **governança pública**, oferecendo ferramentas que apoiem a **visualização, análise e projeção de gastos** contratuais firmados por órgãos governamentais.

A proposta surgiu a partir de um **projeto de extensão**, no qual foi identificado que:

* Há **baixa governança e controle** sobre contratações públicas;
* As **projeções de gastos muitas vezes se distanciam** significativamente da execução real;
* Falta um sistema acessível que possibilite aos gestores públicos visualizarem de forma fácil e intuitiva os dados para tomada de dicisão.

---

### Tecnologias Utilizadas

| Componente            | Descrição                                                                                          |
| --------------------- | -------------------------------------------------------------------------------------------------- |
| **Python + Selenium** | Coleta automatizada dos dados diretamente do site [Compras.gov.br](https://www.comprasnet.gov.br/) |
| **Pandas**            | Limpeza e preparação dos dados                                                                     |
| **SQL Server**        | Armazenamento estruturado dos contratos extraídos                                                  |
| **Power BI**          | Visualização interativa e análise preditiva para gestores públicos                                 |

---

### Funcionalidades Desenvolvidas

* Extração automatizada de contratos com base em filtros de vigência
* Salvamento dos arquivos em `.xlsx` e inserção no banco de dados (SQL Server)
* Estruturação e normalização dos dados para análise posterior
* Integração com dashboards Power BI para análise exploratória e preditiva

---

### Público-alvo

Este projeto é voltado a:

* Gestores públicos
* Estudantes de Análise e Desenvolvimento de Sistemas
* Pesquisadores em transparência, controle público e ciência de dados
* Cidadãos interessados em acompanhar os gastos governamentais de forma crítica e informada

---

### Próximos passos

* [ ] Melhorar o tratamento dos dados
* [ ] Melhorar os painéis afim de evidenciar informações assertivas aos gestores
* [ ] Disponibilizar o dashboard em ambiente online (desenvolver front end?)
* [ ] Criar documentação completa da API de extração

---

### Sobre o projeto

Este repositório é parte do **Projeto Multidisciplinar Integrador (PMI)** do curso de **Análise e Desenvolvimento de Sistemas da Unicesusc**, coordenado por Joice Denise Schäfer.

---

### Contato

Se você tiver sugestões, dúvidas ou quiser colaborar com o projeto, fique à vontade para abrir uma issue ou entrar em contato.
