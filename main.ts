import { parse } from "csv-parse/sync";
import mysql from "mysql";
import fs from "node:fs";
import { z } from "zod";
import { execSync } from "node:child_process";
import * as uuid from "uuid";

function main() {
  const data = fs.readFileSync("data.csv");

  const parsed = parse(data, {
    delimiter: ",",
    columns: true,
    skip_empty_lines: true,
  });

  const generatedContentRows: GeneratedContentRow[] = [];
  const generatedContentAttributeRows: GeneratedContentAttributeRow[] = [];

  let errors = 0;
  let i = 0;
  for (const { skill, question, standardIdParams } of skillQuestionPairs(
    parsed
  )) {
    try {
      const contentId = uuid.v4();
      generatedContentRows.push({
        id: contentId,
        requestedContext: {},
        content: parseQuestion(question),
        contentGeneratorConfigIdQuery: contentGeneratorConfigIdQuery(),
        standardIdQuery: standardIdQuery(standardIdParams),
        status: "training",
      });
      generatedContentAttributeRows.push({
        id: uuid.v4(),
        attributeName: "Skill",
        attributeValue: skill,
        generatedContentId: contentId,
      });

      i++;
    } catch (e) {
      errors++;
      console.error("Failed to parse question", question);
    }
    console.log("row ", i);
    console.info("--------");
  }
  console.error("total errors", errors);

  fs.writeFileSync("up0.sql", insertGeneratedContent(generatedContentRows));

  fs.writeFileSync(
    "up1.sql",
    insertGeneratedContentAttributes(generatedContentAttributeRows)
  );

  fs.writeFileSync("down0.sql", deleteGeneratedContent(generatedContentRows));

  fs.writeFileSync(
    "down1.sql",
    deleteGeneratedContentAttributes(generatedContentAttributeRows)
  );
}

function* skillQuestionPairs(rows: Record<string, string>[]) {
  const seen = new Set<string>();
  for (const row of rows) {
    for (const i of [1, 2, 3, 4, 5, 6]) {
      for (const j of [1, 2, 3]) {
        const standardExternalIdL3 = row["Standard Id (L3)"];
        const standardExternalIdL2 = row["Standard Id (L2)"];
        const standardExternalIdL1 = row["Standard Id (L1)"];

        const standardExternalId =
          standardExternalIdL3 !== ""
            ? standardExternalIdL3
            : standardExternalIdL2 !== ""
            ? standardExternalIdL2
            : standardExternalIdL1;
        const data = {
          skill: row[`Skill ${i}`],
          question: row[`Skill ${i} Question ${j}`],
          standardIdParams: {
            standardExternalId,
            clusterExternalId: row["Cluster Id"],
            domainExternalId: row["Domain Id"],
            courseName: row["Course"],
            subjectName: row["Subject"],
          },
        };
        const seenKey = `${standardExternalId}||||${data.skill}||||${data.question}`;
        if (data.skill && data.question && !seen.has(seenKey)) {
          seen.add(seenKey);
          yield data;
        }
      }
    }
  }
}

function startTransaction() {
  return "BEGIN";
}

function commitTransaction() {
  return "COMMIT";
}

type StandardIdQueryParams = {
  standardExternalId: string;
  clusterExternalId: string;
  domainExternalId: string;
  courseName: string;
  subjectName: string;
};

function standardIdQuery({
  standardExternalId,
  clusterExternalId,
  domainExternalId,
  courseName,
  subjectName,
}: StandardIdQueryParams) {
  const query = `
SELECT
	s.id
FROM
	standards s
	JOIN standard_clusters sc ON s.cluster_id = sc.id
	JOIN standard_domains sd ON sc.domain_id = sd.id
	JOIN subjects sub ON sd.subject_id = sub.id
	JOIN courses c ON c.subject_id = sub.id
WHERE
	s.external_id = ?
	AND sc.external_id = ?
	AND sd.external_id = ?
	AND c.name = ?
	AND sub.name = ?
  `
    .replace(/\n/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const formattedQuery = mysql.format(query, [
    standardExternalId,
    clusterExternalId,
    domainExternalId,
    courseName,
    subjectName,
  ]);
  return mysql.raw(`(${formattedQuery})`);
}

const cache = new Map<string, Question>();

function parseQuestion(input: string): Question {
  if (cache.get(input)) {
    return cache.get(input)!;
  }

  const stdout = execSync("python parser/main.py", { input }).toString();
  let res: any;
  try {
    res = JSON.parse(stdout);
  } catch (e) {
    console.log(stdout);
    throw e;
  }
  const question = {
    question: res["question_header"]
      ? res["question_header"] + res["question_body"]
      : res["question_body"],
    answer_options: ["A", "B", "C", "D"].map((id) => ({
      id,
      answer: res[`option_${id}_answer`],
      correct: res[`option_${id}_correct`]
        ? res[`option_${id}_correct`]?.trim() === "True"
        : undefined,
      explanation: res[`option_${id}_explanation`],
    })),
  };
  for (const option of question.answer_options) {
    if (option.explanation === "Correct: False") {
      option.correct = false;
      option.explanation = "";
    }
    if (option.explanation === "Correct: True") {
      option.correct = true;
      option.explanation = "";
    }
  }

  const res2 = Question.safeParse(question);
  if (!res2.success) {
    console.error(question);
    throw new Error("Failed to parse question");
  }
  cache.set(input, res2.data);
  return res2.data;
}

function contentGeneratorConfigIdQuery() {
  return mysql.raw(
    `(SELECT id FROM content_gen_content_generator_configs WHERE external_id = 'MCQ Example Content')`
  );
}

const QuestionOption = z.object({
  id: z.string().trim().min(1),
  answer: z.string().trim().min(1),
  correct: z.boolean().optional(),
  explanation: z.string().trim().default(""),
});
type QuestionOption = z.infer<typeof QuestionOption>;

const Question = z.object({
  question: z.string().trim().min(1),
  answer_options: z.array(QuestionOption).length(4),
});
type Question = z.infer<typeof Question>;

type GeneratedContentRow = {
  id: string;
  requestedContext: {}; // Always set this to an empty json object
  content: Question;
  contentGeneratorConfigIdQuery: ReturnType<typeof mysql.raw>; // TBD when the other PR is merged
  standardIdQuery: ReturnType<typeof mysql.raw>;
  status: "training";
};

function insertGeneratedContent(rows: GeneratedContentRow[]) {
  return mysql.format(
    "INSERT IGNORE INTO content_gen_generated_content (id, content, content_generator_config_id, standard_id, status) VALUES ?",
    [
      rows.map((row) => [
        row.id,
        JSON.stringify(row.content),
        row.contentGeneratorConfigIdQuery,
        row.standardIdQuery,
        row.status,
      ]),
    ]
  );
}

type GeneratedContentAttributeRow = {
  id: string;
  attributeName: "Skill";
  attributeValue: string;
  generatedContentId: string;
};

function insertGeneratedContentAttributes(
  rows: GeneratedContentAttributeRow[]
) {
  return mysql.format(
    "INSERT IGNORE INTO content_gen_generated_content_attributes (id, attribute_name, attribute_value, attribute_hash, generated_content_id) VALUES ?",
    [
      rows.map((row) => [
        row.id,
        row.attributeName,
        row.attributeValue,
        mysql.raw(`SHA2(${mysql.escape(row.attributeValue)}, 256)`),
        row.generatedContentId,
      ]),
    ]
  );
}

function deleteGeneratedContent(rows: GeneratedContentRow[]) {
  return mysql.format(
    "DELETE FROM `content_gen_generated_content` WHERE id IN (?)",
    [rows.map((row) => row.id)]
  );
}

function deleteGeneratedContentAttributes(
  rows: GeneratedContentAttributeRow[]
) {
  return mysql.format(
    "DELETE FROM `content_gen_generated_content_attributes` WHERE id IN (?)",
    [rows.map((row) => row.id)]
  );
}

main();
