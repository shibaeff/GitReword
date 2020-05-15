from argparse import ArgumentParser
import sys

from library import Repo
import config

def commit_range(oldest, edge_commit):
    """Получим лист коммитов"""
    commits = []
    while edge_commit != oldest:
        commits.append(edge_commit)
        edge_commit = edge_commit.prev()
    commits = commits[::-1]
    return commits


def update_head(reference, new_commit):
    # обновляем голову, чтобы показывала на новое значение
    target_oid = reference.target.oid if reference.target else Oid.null()
    print(f"Было {reference.name} ({target_oid} стало {new_commit.oid})")
    reference.update(new_commit, "git-revise rewrite")



def parser():
    parser = ArgumentParser(
        description="""\
        Эта штука поможет тебе убирать маты в коммитах, не используя медленный rebase.
        """
    )
    parser.add_argument("target", nargs="?", help="целевой коммит")
    parser.add_argument("--parallel", action="store_true", help="Запустить изменения паралельно")

    parser.add_argument("--ref", default="HEAD", help="целевая ссылочка")
    parser.add_argument("--file", default="", help="файл для считывания информации")

    mode_group = parser.add_mutually_exclusive_group()

    mode_group.add_argument(
        "--message",
        "-m",
        action="append",
        help="укажеите сообщение для коммита",
    )

    return parser

def single_commit(repo, target, c_message, reference):
    # Смотрим ссылку, которую будем обновлять
    h = repo.get_commit_ref(reference)
    if h.target is None:
        raise ValueError("Ссылка не была найдена!")
    assert h.target is not None

    if target is None:
        raise ValueError("Вы не указали целевой коммит")

    h = repo.get_commit_ref(reference)
    if h.target is None:
        raise ValueError("Ваш коммит не известен")

    cur = intial = repo.get_obj(target)
    rebase_list = commit_range(cur, h.target)

    # Изменить сообщение
    if c_message:
        message = b"\n".join(l.encode("utf-8") + b"\n" for l in c_message)
        cur = cur.update(message=message)

    # Если что-то изменили, то выполняем измнение
    if cur != intial:
        print(cur.oid.short(), cur.summary())
        # делаем необходимые для изменения ребейзы
        for commit in rebase_list:
            print("Делаем ребейз")
            cur = commit.rebase(cur)
            print(cur.oid.short(), cur.summary())

        update_head(h, cur)
    else:
        print("Никаких изменений!", file=sys.stderr)


def main():
    args = parser().parse_args()
    if args.parallel:
        config.PARALLEL = True
    with Repo() as repo:
        if args.message:

            print("Работаем в режиме одиночной правки")
            single_commit(repo, args.target, args.message, args.ref)
            return
        if args.file:
            print("Работаем в режиме считывания правок из файла", args.file)
            arglist = []
            with open(args.file, "r") as f:
                print("Файл найден")
                for line in f.readlines():
                    target, message = line.split()
                    single_commit(repo, target, message, "HEAD")
            return




# такой запуск из-за сборки PyInstaller)))
main()
