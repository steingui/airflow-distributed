import os
from fpdf import FPDF

class CalangoReport(FPDF):
    def header(self):
        # Header bar
        self.set_fill_color(31, 58, 86)  # Deep Calango Slate/Blue
        self.rect(0, 0, 210, 15, 'F')
        
        # Header text
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 10)
        self.cell(0, -5, "CALANGO INVESTIMENTOS - DEPARTAMENTO DE ENGENHARIA DE DADOS", align="R")
        self.ln(12)
        
    def footer(self):
        # Footer text
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 10, f"Calango Investimentos | Relatório Técnico - Airflow Distribuído | Página {self.page_no()}/{{nb}}", align="C")

def create_report():
    pdf = CalangoReport()
    pdf.alias_nb_pages()
    pdf.set_margins(15, 20, 15)
    
    # ------------------ PÁGINA 1: CAPA E INTRODUÇÃO ------------------
    pdf.add_page()
    pdf.ln(10)
    
    # Title
    pdf.set_text_color(31, 58, 86)
    pdf.set_font("Helvetica", "B", 24)
    pdf.cell(0, 15, "Relatório Técnico", ln=True, align="L")
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 10, "Implantação e Escalonamento de Pipeline", ln=True, align="L")
    pdf.cell(0, 10, "Apache Airflow Distribuído com Celery Executor", ln=True, align="L")
    
    # Divider
    pdf.set_fill_color(31, 58, 86)
    pdf.rect(15, pdf.get_y() + 5, 180, 2, 'F')
    pdf.ln(15)
    
    # Metadata
    pdf.set_text_color(50, 50, 50)
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(100, 7, "Arquiteto de Soluções: steingui (Guilherme Steink)", ln=False)
    pdf.cell(80, 7, "Data: 02 de Julho de 2026", ln=True, align="R")
    pdf.cell(100, 7, "Empresa: Calango Investimentos S.A.", ln=False)
    pdf.cell(80, 7, "Ambiente: Desenvolvimento / Homologação", ln=True, align="R")
    pdf.ln(10)
    
    # Context
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "1. Contexto e Motivação do Projeto", ln=True)
    pdf.set_font("Helvetica", "", 10)
    
    intro_text = (
        "A Calango Investimentos, fintech atuante no mercado de compra e venda de créditos de pequenas "
        "bandeiras de cartão de crédito, expandiu recentemente suas operações adquirindo 70% de participação em um "
        "marketplace. O MVP original realizava a captura de dados via endpoint e alimentava uma arquitetura "
        "medalhão. Contudo, visando a escalabilidade necessária para a nova dimensão de negócios, projetou-se um "
        "pipeline distribuído rodando no Apache Airflow com Celery Executor.\n\n"
        "O pipeline foi desenhado para coletar de forma assíncrona e paralela os dados das entidades fundamentais do "
        "marketplace (Users, Products e Carts) através da FakeStore API, gravando-os em tabelas relacionais em banco "
        "de dados PostgreSQL dedicado, garantindo robustez e performance."
    )
    pdf.multi_cell(0, 5, intro_text)
    pdf.ln(10)
    
    # Architecture
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "2. Arquitetura da Solução Distribuída", ln=True)
    pdf.set_font("Helvetica", "", 10)
    
    arch_text = (
        "- Airflow Webserver & Scheduler: Orquestração e monitoramento das execuções.\n"
        "- Celery Executor: Distribuição de tasks para processamento concorrente.\n"
        "- Redis (Message Broker): Gerenciador da fila de mensagens enviadas aos Workers.\n"
        "- PostgreSQL: Repositório de metadados do Airflow e banco de dados de destino do Marketplace.\n"
        "- Celery Workers: Executores das tarefas distribuídos em containers Docker escaláveis.\n"
        "- Flower: Ferramenta de monitoramento em tempo real do ecossistema Celery."
    )
    pdf.multi_cell(0, 5, arch_text)
    
    # ------------------ PÁGINA 2: TASKFLOW E EXECUÇÃO COM 3 WORKERS ------------------
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "3. O Pipeline de Dados (TaskFlow API)", ln=True)
    pdf.set_font("Helvetica", "", 10)
    
    dag_desc = (
        "O desenvolvimento do pipeline seguiu rigidamente as diretrizes da TaskFlow API do Airflow, utilizando as "
        "anotações @dag e @task. As dependências entre as tarefas foram criadas de forma implícita a partir do retorno "
        "de valores (XComs automáticos), assegurando que:\n"
        "  1. A criação de tabelas ocorra primeiro (create_tables).\n"
        "  2. A ingestão de Users, Products e Carts ocorra 100% em paralelo, distribuída entre os Workers Celery.\n"
        "  3. A consolidação dos resultados (notify_success) ocorra somente após a conclusão do bloco paralelo."
    )
    pdf.multi_cell(0, 5, dag_desc)
    pdf.ln(5)
    
    # 3 Workers Execution
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "4. Execução da Atividade - Etapa 1: Cluster com 3 Workers", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5, (
        "Os serviços foram levantados inicialmente fixando 3 réplicas de Workers:\n"
        "Comando: docker compose up -d --scale airflow-worker=3\n"
        "O pipeline foi ativado e todas as tarefas foram processadas com sucesso."
    ))
    pdf.ln(5)
    
    # Embed Airflow graph screenshot
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "Evidência 4.1: Grafo de Execução de Sucesso na UI do Airflow", ln=True)
    pdf.image("evidence_airflow_graph.png", x=15, w=180)
    pdf.ln(5)
    
    # ------------------ PÁGINA 3: FLOWER E O PRIMEIRO SCALE ------------------
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "Evidência 4.2: Dashboard do Flower exibindo os 3 Workers Celery Ativos (Online)", ln=True)
    pdf.image("evidence_flower_workers_3.png", x=15, w=180)
    pdf.ln(10)
    
    # Stop & Dynamic Scale
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "5. Interrupção e Escalonamento Dinâmico (5 Workers)", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5, (
        "Conforme especificado, a execução inicial com 3 workers foi interrompida e os containers destruídos via:\n"
        "Comando: docker compose down\n\n"
        "A estrutura foi então modificada para permitir o escalonamento flexível e dinâmico de instâncias de workers. "
        "Em seguida, levantou-se o cluster especificando 5 workers dinâmicos:\n"
        "Comando: docker compose up -d --scale airflow-worker=5\n"
        "Abaixo é possível visualizar o Flower registrando 5 workers ativos."
    ))
    pdf.ln(5)
    
    # Embed Flower 5 workers screenshot
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "Evidência 5.1: Painel do Flower registrando 5 Workers Celery Ativos", ln=True)
    pdf.image("evidence_flower_workers_5.png", x=15, w=180)
    
    # ------------------ PÁGINA 4: SCALE DOWN PARA 2 WORKERS E BANCO DE DADOS ------------------
    pdf.add_page()
    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "6. Escalonamento Down para 2 Workers (Sem Downtime)", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5, (
        "Com todos os serviços do cluster em execução contínua, reduziu-se a capacidade do cluster de processamento "
        "para apenas 2 workers, sem interrupção do banco de dados postgres, do redis ou da interface web:\n"
        "Comando: docker compose up -d --scale airflow-worker=2\n"
        "O Flower registra que 3 workers anteriores foram desligados (status Offline) e 2 permanecem ativos (Online)."
    ))
    pdf.ln(5)
    
    # Embed Flower 2 workers screenshot
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 6, "Evidência 6.1: Painel do Flower registrando 2 Workers Online e 3 Offline", ln=True)
    pdf.image("evidence_flower_workers_2.png", x=15, w=180)
    pdf.ln(10)
    
    # DB Verification
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "7. Validação de Dados e Banco de Dados de Destino", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5, (
        "Para comprovar a gravação permanente e íntegra dos dados capturados da FakeStore API, executou-se uma query "
        "relacional no Postgres. Os registros gravados com sucesso nas tabelas de destino correspondem a:\n"
        "  - Tabela 'users': 10 registros de usuários cadastrados.\n"
        "  - Tabela 'products': 20 registros de mercadorias catalogadas.\n"
        "  - Tabela 'carts': 7 vendas registradas.\n"
        "  - Tabela 'cart_items': 14 itens individuais vinculados aos carrinhos."
    ))
    pdf.ln(5)
    
    # Query block
    pdf.set_font("Courier", "B", 9)
    pdf.set_fill_color(240, 240, 240)
    pdf.set_text_color(31, 58, 86)
    
    query_text = (
        " $ docker compose exec postgres psql -U airflow -d airflow -c \\\n"
        "   \"SELECT 'users' as tbl, count(*) from users union all \\\n"
        "    select 'products', count(*) from products union all \\\n"
        "    select 'carts', count(*) from carts union all \\\n"
        "    select 'cart_items', count(*) from cart_items;\"\n\n"
        "     tbl     | count \n"
        " ------------+-------\n"
        "  users      |    10\n"
        "  products   |    20\n"
        "  carts      |     7\n"
        "  cart_items |    14\n"
        " (4 rows)"
    )
    pdf.multi_cell(0, 4.5, query_text, border=1, fill=True)
    
    # Save Report
    pdf.output("relatorio_execucao_airflow_distribuido.pdf")
    print("Report PDF generated successfully as 'relatorio_execucao_airflow_distribuido.pdf'.")

if __name__ == "__main__":
    create_report()
