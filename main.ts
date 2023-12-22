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
  let limit = 999999;
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
      if (i > limit) {
        break;
      }
    } catch (e) {
      errors++;
      console.error("Failed to parse question", question);
    }
    console.info("--------");
  }
  console.error("total errors", errors);

  fs.writeFileSync(
    "up.sql",
    [
      startTransaction(),
      insertGeneratedContent(generatedContentRows),
      insertGeneratedContentAttributes(generatedContentAttributeRows),
      commitTransaction(),
    ].join(";\n")
  );

  fs.writeFileSync(
    "down.sql",
    [
      startTransaction(),
      deleteGeneratedContentAttributes(generatedContentAttributeRows),
      deleteGeneratedContent(generatedContentRows),
      commitTransaction(),
    ].join(";\n")
  );
}

function* skillQuestionPairs(rows: Record<string, string>[]) {
  const seen = new Set<string>();
  for (const row of rows) {
    for (const i of [1, 2, 3, 4, 5, 6]) {
      for (const j of [1, 2, 3]) {
        const data = {
          skill: row[`Skill ${i}`],
          question: row[`Skill ${i} Question ${j}`],
          standardIdParams: {
            standardExternalIdL3: row["Standard Id (L3)"],
            standardExternalIdL2: row["Standard Id (L2)"],
            standardExternalIdL1: row["Standard Id (L1)"],
            clusterExternalId: row["Cluster Id"],
            domainExternalId: row["Domain Id"],
            courseName: row["Course"],
            subjectName: row["Subject"],
          },
        };
        const seenKey = `${data.skill}||||${data.question}`;
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
  standardExternalIdL3: string;
  standardExternalIdL2: string;
  standardExternalIdL1: string;
  clusterExternalId: string;
  domainExternalId: string;
  courseName: string;
  subjectName: string;
};

function standardIdQuery({
  standardExternalIdL3,
  standardExternalIdL2,
  standardExternalIdL1,
  clusterExternalId,
  domainExternalId,
  courseName,
  subjectName,
}: StandardIdQueryParams) {
  const standardExternalId =
    standardExternalIdL3 !== ""
      ? standardExternalIdL3
      : standardExternalIdL2 !== ""
      ? standardExternalIdL2
      : standardExternalIdL1;
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

function parseQuestion(input: string): Question {
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

  // if (question.answer_options.every((o) => o.correct != null)) {
  //   console.log(input);
  //   console.log(question);
  // }

  const res2 = Question.safeParse(question);
  if (!res2.success) {
    console.error(question);
    throw new Error("Failed to parse question");
  }
  return res2.data;
}

function contentGeneratorConfigIdQuery() {
  return mysql.raw(
    `(SELECT id FROM content_gen_content_generator_configs WHERE external_id = 'MCQ Example Content')`
  );
}

// function parseQuestionWithSections(input: string): Question {
//   const sections = input.split("\n\n");
//   const [questionSection, ...optionSections] = sections;
//
//   const questionLines = questionSection.split("\n");
//   if (questionLines[0] !== "Question:") {
//     throw new Error("Expected Question: section");
//   }
//   if (questionLines.length != 2) {
//     console.error(questionLines);
//     throw new Error("Question contains newline");
//   }
//   const question = questionLines[1].trim();
//
//   const options: QuestionOption[] = [];
//   for (const [i, option] of ["A", "B", "C", "D"].entries()) {
//     const optionLines = sections[i + 1].split("\n");
//     if (optionLines[0] !== `Option ${option}:`) {
//       throw new Error(`Expected ${option}: section`);
//     }
//     if (optionLines.length != 4) {
//       console.error(optionLines);
//       throw new Error("Option contains newline");
//     }
//
//     if (!optionLines[1].startsWith("Answer:")) {
//       throw new Error(`Expected Answer: section`);
//     }
//     const answer = optionLines[1].replace("Answer:", "").trim();
//
//     if (!optionLines[2].startsWith("Explanation:")) {
//       throw new Error(`Expected Explanation: section`);
//     }
//     const explanation = optionLines[2].replace("Explanation:", "").trim();
//
//     if (!optionLines[3].startsWith("Correct:")) {
//       throw new Error(`Expected Correct: section`);
//     }
//     const correct = optionLines[3].replace("Correct:", "").trim();
//     if (correct !== "True" && correct !== "False") {
//       throw new Error(`Expected True or False for correct`);
//     }
//
//     options.push({
//       id: option,
//       answer,
//       explanation,
//       correct: correct === "True",
//     });
//   }
//
//   return Question.parse({
//     question,
//     answer_options: options,
//   });
// }

const QuestionOption = z.object({
  id: z.string().min(1),
  answer: z.string().min(1),
  correct: z.boolean().optional(),
  explanation: z.string().default(""),
});
type QuestionOption = z.infer<typeof QuestionOption>;

const Question = z.object({
  question: z.string().min(1),
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
    "INSERT INTO content_gen_generated_content (id, content, content_generator_config_id, standard_id, status) VALUES ?",
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
    "INSERT INTO content_gen_generated_content_attributes (id, attribute_name, attribute_value, attribute_hash, generated_content_id) VALUES ?",
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
