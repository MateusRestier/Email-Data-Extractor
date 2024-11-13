try:
    import imaplib
    import poplib
    import email
    from email.parser import Parser
    from email.header import decode_header
    import pandas as pd
    import os
    from datetime import datetime
    from email.utils import parsedate_to_datetime
    from datetime import timedelta
    import pyodbc
    import re
except ModuleNotFoundError:
    print("Erro ao importar as bibliotecas. Verifique se as dependências estão corretas e tente novamente.")


"""----------------------- INÍCIO DAS FUNÇÕES DE PROCESSAMENTO DE E-MAIL -----------------------"""


# Lista de remetentes que não devem ser marcados como CorrecaoDeNota, e que serão excluídos como remetente caso a origem seja central de notas
remetentes_excluidos = os.getenv("REMETENTES_EXCLUIDOS", "").split(",")


# Funcao principal para orquestrar o processo para um email específico
def process_email_account(email_user, email_pass, email_label, data_formatada):
    # Tentar conexão via IMAP
    mail, protocol = connect_to_imap(email_user, email_pass)
    if mail is None:
        # Se a conexão IMAP falhar, tentar via POP3
        mail, protocol = connect_to_pop(email_user, email_pass)
        if mail is None:
            print(f"Falha ao conectar via IMAP e POP3 para {email_user}.")
            return []

    email_data = []

    if protocol == 'IMAP':
        message_ids = fetch_emails_imap(mail, data_formatada)
        for num in message_ids:
            email_info = process_email_imap(mail, num, email_label)  # Passar email_label aqui
            if email_info:
                email_info.append(email_label)  # Adicionar a OrigemDoEmail
                email_data.append(email_info)
    elif protocol == 'POP3':
        num_emails = len(mail.list()[1])
        emails = fetch_emails_pop(mail, num_emails, data_formatada)
        for msg in emails:
            email_info = process_email(msg, email_label)  # Passar email_label aqui
            if email_info:
                email_info.append(email_label)  # Adicionar a OrigemDoEmail
                email_data.append(email_info)

    if protocol == 'IMAP':
        mail.logout()
    else:
        mail.quit()

    return email_data


# Funcao para tentar conexão IMAP
def connect_to_imap(email_user, email_pass):
    servidores_imap = [
        "imap.gmail.com",
        "outlook.office365.com",
        "imap-mail.outlook.com",
        "smtp-mail.outlook.com",
        "imap.outlook.com"
    ]
    
    for servidor in servidores_imap:
        print(f"\n\n\nTentando conexão IMAP com o servidor: {servidor} e email: {email_user} senha: {email_pass}")
        try:
            mail = imaplib.IMAP4_SSL(servidor)
            mail.login(email_user, email_pass)
            print(f"\nConectado com sucesso à conta {email_user} via IMAP no servidor {servidor}")
            return mail, 'IMAP'
        except imaplib.IMAP4.error as e:
            print(f"Erro de autenticacao IMAP com o servidor {servidor}: {e}")
        except Exception as e:
            print(f"Erro desconhecido ao tentar o servidor IMAP {servidor}: {e}")
    print("\nFalha ao conectar via IMAP.")
    return None, None


# Funcao para tentar conexão POP3
def connect_to_pop(email_user, email_pass):
    servidores_pop3 = [
        "pop.gmail.com",
        "smtp-mail.outlook.com",
        "outlook.office365.com",
        "pop-mail.outlook.com",
        "pop3.live.com",
        "pop3.outlook.com"
    ]
    for servidor in servidores_pop3:
        print(f"\n\n\nTentando conexão POP3 com o servidor: {servidor} e email: {email_user} senha: {email_pass}")
        try:
            mail = poplib.POP3_SSL(servidor)
            mail.user(email_user)
            mail.pass_(email_pass)
            print(f"\nConectado com sucesso à conta {email_user} via POP3 no servidor {servidor}")
            return mail, 'POP3'
        except poplib.error_proto as e:
            print(f"Erro de autenticacao POP3 com o servidor {servidor}: {e}")
        except Exception as e:
            print(f"Erro desconhecido ao tentar o servidor POP3 {servidor}: {e}")
    print("\nFalha ao conectar via POP3.\n")
    return None, None


# Funcao para buscar e-mails por data específica via IMAP
def fetch_emails_imap(mail, date):
    mail.select("inbox")
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    imap_date = date_obj.strftime("%d-%b-%Y")
    status, messages = mail.search(None, f'(ON {imap_date})')
    return messages[0].split()


# Funcao para buscar e-mails por data específica via POP3
def fetch_emails_pop(mail, num_emails, target_date):
    email_data = []
    for i in range(num_emails):
        raw_email = b"\n".join(mail.retr(i + 1)[1]).decode('utf-8')
        msg = Parser().parsestr(raw_email)

        # Processar e verificar a data
        email_date = parsedate_to_datetime(msg['Date']).strftime('%Y-%m-%d')
        if email_date == target_date:
            email_data.append(msg)
    return email_data


# Funcao para processar um e-mail e extrair os dados para IMAP
def process_email_imap(mail, num, email_label):
    status, msg_data = mail.fetch(num, "(RFC822)")
    for response_part in msg_data:
        if isinstance(response_part, tuple):
            msg = email.message_from_bytes(response_part[1])
            return process_email(msg, email_label)  # Passar email_label aqui


# Função para processar um e-mail e extrair os dados (comum para ambos IMAP e POP3)
def process_email(msg, email_label):
    # Decodificar o assunto
    subject, encoding = decode_header(msg["Subject"])[0]
    if isinstance(subject, bytes):
        try:
            subject = subject.decode(encoding if encoding else "utf-8")
        except UnicodeDecodeError:
            subject = subject.decode(encoding if encoding else "windows-1252")

    # Remover os prefixos 'RE:', 'RES:' e 'ENC:' do assunto
    prefixos = ['RE:', 'RES:', 'ENC:', 'Resposta automática:', 'Não é possível entregar:', 'Relatório de Recall de Mensagem para a mensagem:', 'Assunto:']
    for prefixo in prefixos:
        if subject.upper().startswith(prefixo):
            subject = subject[len(prefixo):].strip()

    # Decodificar o remetente
    sender, encoding = decode_header(msg["From"])[0]
    if isinstance(sender, bytes):
        try:
            sender = sender.decode(encoding if encoding else "utf-8")
        except UnicodeDecodeError:
            sender = sender.decode(encoding if encoding else "windows-1252")

    # Extraia apenas o e-mail do campo From (se houver nome junto com o e-mail)
    if '<' in sender and '>' in sender:
        sender = sender.split('<')[1].replace('>', '').strip()
    else:
        sender = sender.strip()

    # Processar a data do e-mail
    date_received = msg["Date"]

    attachment_count = 0
    attachment_extensions = []
    attachment_names = []
    nf_count = 0
    nf_files = []
    compressed_count = 0

    for part in msg.walk():
        if part.get_content_disposition() == 'attachment':
            attachment_count += 1
            filename = part.get_filename()

            # Verificar se o nome do arquivo é válido
            if filename:
                # Decodificar o nome do arquivo se necessário
                decoded_filename, encoding = decode_header(filename)[0]
                if isinstance(decoded_filename, bytes):
                    try:
                        filename = decoded_filename.decode(encoding if encoding else 'utf-8')
                    except UnicodeDecodeError:
                        filename = decoded_filename.decode(encoding if encoding else 'windows-1252')

                attachment_names.append(filename)

                # Extrair a extensão do arquivo e corrigir se houver malformacao
                extension = os.path.splitext(filename)[1][1:].lower()

                if extension.endswith('?='):
                    extension = extension.replace('?=', '')

                attachment_extensions.append(extension)

                if "nf" in filename.lower():
                    nf_count += 1
                    nf_files.append(filename)

                if extension in ['zip', 'rar']:
                    compressed_count += 1
            else:
                print(f"Anexo encontrado, mas sem nome de arquivo.")
                attachment_names.append("Sem nome")
                attachment_extensions.append("desconhecido")

    extensions_str = ','.join(attachment_extensions) if attachment_extensions else 'Nenhum'
    attachment_names_str = ' | '.join(attachment_names) if attachment_names else 'Nenhum'
    nf_files_str = ', '.join(nf_files) if nf_files else 'Nenhum'

    return [subject, date_received, sender, attachment_count, nf_count, extensions_str, compressed_count, attachment_names_str, nf_files_str]


"""----------------------- INÍCIO DAS FUNÇÕES DE TRATAMENTO DE DADOS -----------------------"""


# Funcao para formatar a data
def formatar_data(data):
    try:
        data_formatada = datetime.strptime(data, "%Y%m%d").strftime("%Y-%m-%d")
        return data_formatada
    except ValueError:
        return None


def extrair_notas_fiscais(assunto):
    """
    Função que extrai os números das notas fiscais do assunto do e-mail e adiciona zeros à esquerda até completar 9 dígitos.
    Também identifica casos onde o termo "diversos" ou variações (como "diversas") aparece no lugar dos números.

    A função cobre os seguintes casos:
    - VCTO 16/09 - NF Nº 397894 - 013120 - UP HEALTH
    - VCTO 09/09 - NF N° 27 - 020094 - ELISABETE BOENO
    - Venc 15/10/24 NF 3425 CC 010013 /015700 Mercado Livre
    - NF 01 - CC 030018  - Vencimento 02/10 - Francisco de Assis Braz
    - Venc 15/10/24 NF 030474 CC 010013 /  015555 Mercado Livre
    - NF 113 | CC 014210 | VENC 12/08/2024 | FORNECEDOR ADALBERTO SILVA DE FREITAS
    - NOTA FISCAL PEDENTE LOJA F322 NF283144
    - NF diversas - CC 011400 - Pagamento 03/10 - Camilo dos Santos 
    - NF 69795 69793 e 69794 - CC 011400 - Vencimentos 02/10, 01/11 e 01/12 - Intelipost Consultoria e Tecnologia de Log
    - JAE Ilha – NF 671828 - VCTO  20/09, 08/10 - CC/R 020104 – CC 010013
    - Comercial -  NF 953 – NF 9534 – NF 9514 – NF 9529- NF 9526- NF 9537- NF 9539 – NF 9545- NF 9527  – CC/R 015200 – CC 010013
    - VCTO 05/09 - NF N° 361561 / 362057 / 362336 / 362653 / 362788 / 363231 / 364210 / 361337 / 361706 / 362205 / 362691 / 362742 / 363084 / 363998 / 361301 - 011600 - CONSTRULAR

    - Casos com separadores como vírgula, espaço, barra ou a palavra 'e' para múltiplos números de notas fiscais.

    Retorna:
    - Lista de notas fiscais com 9 dígitos, ou a string "Diversas" quando aplicável.
    """
    
    # Verificar se o valor de 'assunto' é uma string válida
    if not isinstance(assunto, str) or assunto.strip() == '':
        return ['Na']  # Retorna um valor padrão se o assunto estiver vazio ou não for string

    # Expressão regular para capturar números das notas fiscais ou o termo "diversos/diversas"
    padrao = r'NF\s*(?:N[º°]?\s*[\s.:]*)?([\d]+(?:[\s/,e]*[\d]+)*|divers\w*)'

    # Procurar pelos números das notas fiscais no assunto, ignorando maiúsculas/minúsculas
    matches = re.findall(padrao, assunto, re.IGNORECASE)

    # Lista para armazenar as notas fiscais com 9 dígitos ou a palavra "Diversas"
    notas_fiscais_completas = []

    # Iterar sobre os matches encontrados
    for match in matches:
        # O grupo capturado é o número da NF ou a palavra "diversos/diversas"
        numeros_brutos = match

        # Se o termo for "diversos/diversas", adiciona "Diversas" à lista
        if re.search(r'divers\w*', numeros_brutos, re.IGNORECASE):
            notas_fiscais_completas.append('Diversas')
        else:
            # Dividir os números por vírgula, espaço, barra ou a palavra "e"
            numeros = re.split(r'[,\s/e]+', numeros_brutos)

            # Adicionar zeros à esquerda para que todos os números tenham 9 dígitos
            for nf_numero in numeros:
                if nf_numero.strip().isdigit():
                    notas_fiscais_completas.append(nf_numero.strip().zfill(9))  # Zeros à esquerda

    # Retornar os números das notas fiscais como uma lista ou uma string vazia se não houver notas
    return notas_fiscais_completas if notas_fiscais_completas else ['Na']


def extrair_centro_custo(assunto):
    """
    Função que extrai o centro de custo no assunto do e-mail.

    O centro de custo é identificado como o primeiro conjunto de 5 ou 6 dígitos que aparece no assunto
    precedido por 'CC/R'. Se 'CC/R' não estiver presente, será extraído o número precedido por 'CC'.
    
    Retorna:
    - Centro de custo com 6 dígitos, preenchido com zeros à esquerda se necessário, ou 'Na' se não houver centro de custo.
    """
    
    if not isinstance(assunto, str) or assunto.strip() == '':
        return 'Na'  # Retorna 'Na' se o assunto estiver vazio ou não for uma string
    
    # Expressão regular para capturar centro de custo precedido por 'CC/R' ou 'CC'
    padrao_prioridade = r'CC/R\s*(\d{5,6})'
    padrao_cc = r'CC\s*(\d{5,6})'
    
    # Primeiro, procurar por centro de custo após 'CC/R'
    match_prioridade = re.search(padrao_prioridade, assunto, re.IGNORECASE)
    
    # Se 'CC/R' for encontrado, retornar o centro de custo correspondente
    if match_prioridade:
        return match_prioridade.group(1).zfill(6)
    
    # Se 'CC/R' não for encontrado, procurar por centro de custo após 'CC'
    match_cc = re.search(padrao_cc, assunto, re.IGNORECASE)
    
    # Se encontrado, retornar o centro de custo correspondente
    if match_cc:
        return match_cc.group(1).zfill(6)
    
    # Caso nenhum padrão seja encontrado, retornar 'Na'
    return 'Na'


def concatenar_idcc(row):
    """
    Função para concatenar a 'IDnotas' e 'CentroCusto'.
    Cada linha deve conter apenas uma combinação de IDnotas e CentroCusto.
    Se 'IDnotas' ou 'CentroCusto' for 'Na', retorna 'Na'.
    """
    # Obter a nota fiscal (remover colchetes se estiver como lista)
    id_notas = ''.join(row['IDnotas']) if isinstance(row['IDnotas'], list) else row['IDnotas']
    
    if id_notas == 'Na' or row['CentroCusto'] == 'Na':
        return 'Na'
    else:
        # Concatenar apenas a nota fiscal com o centro de custo
        return f"{row['CentroCusto']}{id_notas}"


def dividir_linhas_por_nota(df):
    # Lista para armazenar as novas linhas
    novas_linhas = []

    # Iterar sobre o DataFrame linha por linha
    for index, row in df.iterrows():
        # Obter a lista de notas fiscais
        notas_fiscais = extrair_notas_fiscais(row['Assunto'])

        if notas_fiscais:  # Se houver notas fiscais
            for i, nf in enumerate(notas_fiscais):
                # Para cada nota fiscal, criar uma nova linha com os mesmos dados da original
                nova_linha = row.copy()
                nova_linha['IDnotas'] = nf  # Definir a nota fiscal na coluna IDnotas
                
                # Adicionar o sufixo "(2)", "(3)", etc., ao assunto se houver mais de uma nota
                if i > 0:
                    nova_linha['Assunto'] = f"{row['Assunto']} ({i + 1})"
                
                novas_linhas.append(nova_linha)
        else:
            # Se não houver notas fiscais, manter a linha original
            novas_linhas.append(row)

    # Criar um novo DataFrame com as novas linhas
    return pd.DataFrame(novas_linhas)


def tratamento_dados(email_data):
    # Transformar os dados em um DataFrame
    df = pd.DataFrame(email_data, columns=[
        'Assunto', 'Data', 'Remetente', 'Anexos', 'NotasFiscais', 'Extensoes', 
        'ArquivosComprimidos', 'NomesDosArquivos', 'ArquivosNF', 'OrigemDoEmail'
    ])
    
    # Convertendo Datas
    df['Data'] = pd.to_datetime(df['Data'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')
    
    # Remover linhas onde a coluna 'Data' está vazia ou inválida
    df = df.dropna(subset=['Data'])

    # Definir o tamanho máximo permitido para as colunas de strings
    max_length = 255
    
    # Limitar o tamanho das strings e substituir strings vazias por None
    string_columns = ['Assunto', 'Remetente', 'Extensoes', 'NomesDosArquivos', 'ArquivosNF', 'OrigemDoEmail']
    
    for col in string_columns:
        # Truncar strings que excedem o tamanho máximo permitido
        df[col] = df[col].apply(lambda x: x[:max_length] if isinstance(x, str) else x)
        # Substituir strings vazias por None
        df[col] = df[col].replace(r'^\s*$', None, regex=True)
    
    # Remover emails enviados pelos remetentes excluídos
    global remetentes_excluidos
    df = df[~((df['Remetente'].isin(remetentes_excluidos)) & (df['OrigemDoEmail'] == 'Central de Notas'))]

    # operações de extração de notas
    df['IDnotas'] = df['Assunto'].apply(extrair_notas_fiscais)
    df['CentroCusto'] = df['Assunto'].apply(extrair_centro_custo)
    df = dividir_linhas_por_nota(df)
    df['IDCC'] = df.apply(concatenar_idcc, axis=1)
    
    return df


"""----------------------- INÍCIO DAS FUNÇÕES DE JOGAR NO BANCO -----------------------"""


conn_string = (
    f"DRIVER={os.getenv('DB_DRIVER')};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')};"
)


def inserir_no_banco(df, conn_string):
    # Criar a conexão com o banco de dados
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    # Iterar sobre as linhas do DataFrame e inserir no banco de dados
    for index, row in df.iterrows():
        # Executar a inserção no banco de dados diretamente
        cursor.execute('''
            INSERT INTO PF_MapeamentoNotas (Assunto, Data, Remetente, Anexos, NotasFiscais, 
                                            Extensoes, ArquivosComprimidos, NomesDosArquivos, ArquivosNF, OrigemDoEmail, 
                                            IDnotas, CentroCusto, IDCC)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', 
        row['Assunto'], 
        row['Data'], 
        row['Remetente'], 
        row['Anexos'],  # Inserir diretamente do DataFrame
        row['NotasFiscais'], 
        row['Extensoes'], 
        row['ArquivosComprimidos'], 
        row['NomesDosArquivos'], 
        row['ArquivosNF'], 
        row['OrigemDoEmail'],
        row['IDnotas'],
        row['CentroCusto'],
        row['IDCC']
        )

    # Commitar as transações e fechar a conexão
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Dados inseridos no banco de dados com sucesso.")


def remover_duplicatas_do_banco(conn_string):
    try:
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()

        # Excluir duplicatas, mantendo apenas a primeira ocorrência com base na nova lógica
        delete_query = '''
            WITH CTE AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY Assunto
                        ORDER BY ID ASC
                    ) AS row_num
                FROM PF_MapeamentoNotas
            )
            DELETE FROM CTE WHERE row_num > 1;
        '''
        print("Removendo linhas duplicadas da tabela PF_MapeamentoNotas")
        cursor.execute(delete_query)
        conn.commit()
        cursor.close()
        conn.close()

        print("Linhas duplicadas removidas da tabela PF_MapeamentoNotas com sucesso.")
    except Exception as e:
        print(f"Erro ao remover duplicatas: {e}")


"""----------------------- INÍCIO DA FUNÇÃO PRINCIPAL -----------------------"""


def main():
    print("\n\nＭａｐｅａｍｅｎｔｏ ｄｅ Ｎｏｔａｓ Ｆｉｓｃａｉｓ")

    # Solicitar ao usuário que escolha entre usar a data D-1, um intervalo de datas, ou os últimos 5 dias
    print("\nEscolha uma das opções:")
    print("1. Usar a data D-1")
    print("2. Inserir um intervalo de datas")
    print("3. Inserir um dia específico que deseje consultar")
    print("4. Consultar os últimos 5 dias")

    choice = '4'
    #choice = input("Digite 1, 2, 3 ou 4: ").strip()

    if choice == '1':
        data_formatada = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # Pegar a data de D-1
        date_range = [data_formatada]  # Apenas um dia, sem intervalo
    elif choice == '2':
        start_date = input("Digite a data inicial (YYYYMMDD): ").strip()
        end_date = input("Digite a data final (YYYYMMDD): ").strip()

        # Converter as datas para o formato YYYY-MM-DD
        start_date = formatar_data(start_date)
        end_date = formatar_data(end_date)

        # Gerar uma lista de datas entre a data inicial e final
        date_range = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
    elif choice == '3':
        data_formatada = formatar_data(input("Digite a data (YYYYMMDD): ").strip())
        date_range = [data_formatada]
    elif choice == '4':
        date_range = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(5, 0, -1)]
    else:
        print("Opção inválida. Saindo do programa.")
        return

    # Definir as credenciais dos dois emails
    email_user_1 = os.getenv("EMAIL_USER_1")
    email_pass_1 = os.getenv("EMAIL_PASS_1")
    email_label_1 = "Central de Notas"
    email_user_2 = os.getenv("EMAIL_USER_2")
    email_pass_2 = os.getenv("EMAIL_PASS_2")
    email_label_2 = "Devolucao de Notas"
    
    # Processar os emails para cada data no intervalo
    email_data = []
    for data_formatada in date_range:
        try:
            print(f"\nProcessando data: {data_formatada}")
            email_data_1 = process_email_account(email_user_1, email_pass_1, email_label_1, data_formatada)
            email_data_2 = process_email_account(email_user_2, email_pass_2, email_label_2, data_formatada)

            # Verificar se algum dos dois e-mails tem dados
            if email_data_1 or email_data_2:
                email_data.extend(email_data_1 + email_data_2)
            else:
                print(f"Nenhum dado encontrado para a data {data_formatada}. Continuando...")

        except Exception as e:
            print(f"Erro ao processar a data {data_formatada}: {e}")
            continue

    if not email_data:
        print("Nenhum dado encontrado em nenhuma das datas fornecidas. Nenhum arquivo foi gerado.")
        return

    # Chamar a função de tratamento de dados apenas se houver dados
    df = tratamento_dados(email_data)

    # Após o tratamento, inserir os dados no banco
    inserir_no_banco(df, conn_string)

    # Após o tratamento, chamar a função para remover duplicatas
    remover_duplicatas_do_banco(conn_string)
    
    print(f"\nConexão ao servidor fechada.")

if __name__ == "__main__":
    main()