import { getCustomRepository, getRepository, In } from 'typeorm';
import csvParse from 'csv-parse';
import fs from 'fs';

import TransactionsRepository from '../repositories/TransactionsRepository';

import Transaction from '../models/Transaction';
import Category from '../models/Category';

interface CSVTransaction {
  title: string;
  type: 'income' | 'outcome';
  value: number;
  category: string;
}

class ImportTransactionsService {
  async execute(filePath: string): Promise<Transaction[]> {
    const transactionsRepository = getCustomRepository(TransactionsRepository);
    const categoriesRepository = getRepository(Category);

    // Lê o arquivo
    const contactsReadStream = fs.createReadStream(filePath);

    // Passa algumas configurações, informando que deverá obter os dados a patir
    // da segunda linha do arquivo
    const parsers = csvParse({
      from_line: 2,
    });

    // Determina que o arquivo será lido linha a linha e passa as configurações
    // de como deve ser interpretado como CSV
    const parseCSV = contactsReadStream.pipe(parsers);

    const transactions: CSVTransaction[] = [];
    const categories: string[] = [];

    // Lê cada linha do arquivo
    parseCSV.on('data', async line => {
      // Obtém os dados de uma linha e remove os espaços em branco
      const [title, type, value, category] = line.map((cell: string) =>
        cell.trim(),
      );

      // Caso alguma das informações esteja em branco, ele retorna
      if (!title || !type || !value) return;

      categories.push(category);
      transactions.push({ title, type, value, category });
    });

    // Aguarda todo o arquivo ser lido antes de prosseguir
    await new Promise(resolve => parseCSV.on('end', resolve));

    // Busca todas as categorias passadas que já existem no banco
    const existentCategories = await categoriesRepository.find({
      where: {
        title: In(categories),
      },
    });

    // Dos títulos passados já existentes no banco, pega apenas os títulos
    const existentCategoriesTitles = existentCategories.map(
      (category: Category) => category.title,
    );

    // Pega apenas as categorias que ainda não existem no banco
    // No segundo filter, ele remove as categorias duplicadas
    const addCategoryTitles = categories
      .filter(category => !existentCategoriesTitles.includes(category))
      .filter((value, index, self) => self.indexOf(value) === index);

    // Cria todas as categoria não existentes no banco de dados, de uma única vez
    const newCategories = categoriesRepository.create(
      addCategoryTitles.map(title => ({
        title,
      })),
    );

    await categoriesRepository.save(newCategories);

    const finalCategories = [...newCategories, ...existentCategories];

    const createdTransactions = transactionsRepository.create(
      transactions.map(transaction => ({
        title: transaction.title,
        type: transaction.type,
        value: transaction.value,
        category: finalCategories.find(
          category => category.title === transaction.category,
        ),
      })),
    );

    await transactionsRepository.save(createdTransactions);

    // Apaga o arquivo
    await fs.promises.unlink(filePath);

    return createdTransactions;
  }
}

export default ImportTransactionsService;
