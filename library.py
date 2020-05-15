"""
Классы для работы с ОДБ. Сделаны на основе кода GitPython и еще пары других библиотек
"""

import hashlib
import re
from typing import cast
from pathlib import Path
from enum import Enum
from subprocess import Popen, run, PIPE
from collections import defaultdict
import os.path
import sys
import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def rebase(commit, parent):
    if commit.prev() == parent:
        return commit  # No need to do anything

    tree = merge(
        parent.repo.get_obj(parent.tree_oid),
        commit.prev().repo.get_obj(commit.prev().tree_oid),
        commit.repo.get_obj(commit.tree_oid),
    )
    return tree.repo.new_commit(tree, [parent], commit.message, commit.author)

from joblib import Parallel, delayed
def merge(current, root, other):
    names = set(current.entries.keys()).union(root.entries.keys(), other.entries.keys())
    records = {}
    if config.PARALLEL:
        results = Parallel() (
            delayed(merge_recrods)(
                current.entries.get(name),
                root.entries.get(name),
                other.entries.get(name)
            ) for name in names
        )
        for i, name in enumerate(names):
            if results[i] is not None:
                records[name] = results[i]
    else:
        for name in names:
            merged = merge_recrods(
                current.entries.get(name),
                root.entries.get(name),
                other.entries.get(name),
            )
            if merged is not None:
                records[name] = merged
    return current.repo.new_tree(records)



def merge_recrods(current, root, other):
    if root == current:
        return other  #  никаких изменений root -> current
    if root == other:
        return current  # никаких изменений root -> other
    if current == other:
        return current

    raise ValueError("unknown mode")


class MissingObject(Exception):
    """Исключение на случай, если объекта нет ОДБ"""

    def __init__(self, ref: str):
        Exception.__init__(self, f"Объект {ref}  не существует!!!")




class Oid(bytes):
    """Айдишник объекта в ГИТ"""

    def __new__(cls, b):
        if len(b) != 20:
            raise ValueError("Неправильная длина хеша, должна быть 120")
        return super().__new__(cls, b)  # type: ignore

    @classmethod
    def fromhex(cls, instr):
        """Парсим из hex"""
        return Oid(bytes.fromhex(instr))

    @classmethod
    def null(cls):
        """Вернуть нулевы байты"""
        return cls(b"\0" * 20)

    def short(self):
        """Вернуть короткий хеш"""
        return str(self)[:12]

    @classmethod
    def for_object(cls, tag, body):
        """Хешируем объект по телу и тегу"""
        hasher = hashlib.sha1()
        hasher.update(tag.encode() + b" " + str(len(body)).encode() + b"\0" + body)
        return cls(hasher.digest())

    def __repr__(self):
        return self.hex()

    def __str__(self):
        return self.hex()


class Signature(bytes):
    """Сигнатура пользователя"""


    sig_re = re.compile(
        rb"""
        (?P<name>[^<>]+)<(?P<email>[^<>]+)>[ ]
        (?P<timestamp>[0-9]+)
        (?:[ ](?P<offset>[\+\-][0-9]+))?
        """,
        re.X,
    )

    @property
    def name(self):
        """имя"""
        match = self.sig_re.fullmatch(self)
        assert match, "неправильная подпись"
        return match.group("name").strip()

    @property
    def email(self):
        """почта"""
        match = self.sig_re.fullmatch(self)
        assert match, "неправильная подпись"
        return match.group("email").strip()

    @property
    def timestamp(self):
        """unix время"""
        match = self.sig_re.fullmatch(self)
        assert match, "неправильная подпись"
        return match.group("timestamp").strip()

    @property
    def offset(self):
        """часовой пояс"""
        match = self.sig_re.fullmatch(self)
        assert match, "неправильная подпись"
        return match.group("offset").strip()


class Repo:
    """Класс репо, по хорошему это синглтон, поэтому вызывайте всегда в контексте"""
    def __init__(self, cwd= None):
        self._tempdir = None

        self.workdir = Path(self.git("rev-parse", "--show-toplevel", cwd=cwd).decode())  # вызываем баш, рабдир
        self.gitdir = self.workdir / Path(self.git("rev-parse", "--git-dir").decode())  # гит директория опять зовем баш

        self.default_author = Signature(self.git("var", "GIT_AUTHOR_IDENT"))   # автор и коммитер по умолчанию
        self.default_committer = Signature(self.git("var", "GIT_COMMITTER_IDENT"))

        self._catfile = Popen(
            ["git", "cat-file", "--batch"],
            bufsize=-1,
            stdin=PIPE,
            stdout=PIPE,
            cwd=self.workdir,
        )
        self._objects = defaultdict(dict)

        try:
            self.get_obj(Oid.null())
            raise IOError("cat-file не работает")
        except MissingObject:
            pass

    def git(self, *cmd: str, cwd = None, stdin = None, newline = True, env = None, nocapture = False):
        if cwd is None:
            cwd = getattr(self, "workdir", None)

        cmd = ("git",) + cmd
        prog = run(
            cmd,
            stdout=None if nocapture else PIPE,
            cwd=cwd,
            env=env,
            input=stdin,
            check=True,
        )

        if nocapture:
            return b""
        if newline and prog.stdout.endswith(b"\n"):
            return prog.stdout[:-1]
        return prog.stdout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Выход из контекста
        if self._tempdir:
            self._tempdir.__exit__(exc_type, exc_val, exc_tb)

        self._catfile.terminate()
        self._catfile.wait()


    def new_commit(self, tree, parents, message, author = None, committer = None):
        """Создаем копию коммита в памяти"""
        if author is None:
            author = self.default_author
        if committer is None:
            committer = self.default_committer

        body = b"tree " + tree.oid.hex().encode() + b"\n"
        for parent in parents:
            body += b"parent " + parent.oid.hex().encode() + b"\n"
        body += b"author " + author + b"\n"
        body += b"committer " + committer + b"\n"
        body += b"\n"
        body += message
        return Commit(self, body)

    def new_tree(self, entries) :
        """Создаем объект-дерево без сохранения в репо"""
        def entry_key(pair):
            name, entry = pair
            if entry.mode == Mode.DIR:
                return name + b"/"
            return name

        body = b""
        for name, entry in sorted(entries.items(), key=entry_key):
            body += cast(bytes, entry.mode.value) + b" " + name + b"\0" + entry.oid
        return Tree(self, body)

    def get_obj(self, ref):
        """Получем объек по oid или дескриптору"""
        if isinstance(ref, Oid):
            cache = self._objects[ref[0]]
            if ref in cache:
                return cache[ref]
            ref = ref.hex()

        # Отправим дескриптор
        self._catfile.stdin.write(ref.encode() + b"\n")
        self._catfile.stdin.flush()

        # Получем ответ
        resp = self._catfile.stdout.readline().decode()
        if resp.endswith("missing\n"):
            # Не нашли объект
            raise MissingObject(ref)

        parts = resp.rsplit(maxsplit=2)
        oid, kind, size = Oid.fromhex(parts[0]), parts[1], int(parts[2])
        body = self._catfile.stdout.read(size + 1)[:-1]
        assert size == len(body), "плохой размер"

        # Создаем в памяти объект нужного типа
        if kind == "commit":
            obj = Commit(self, body)
        elif kind == "tree":
            obj = Tree(self, body)
        else:
            raise ValueError(f"Неизвестный тип объекта: {kind}")

        obj.persisted = True
        assert obj.oid == oid, "плохой oid"
        return obj

    def get_obj_ref(self, ref):
        return Reference(GitObj, self, ref)

    def get_commit_ref(self, ref):
        return Reference(Commit, self, ref)

    def get_tree_ref(self, ref: str):
        return Reference(Tree, self, ref)



class GitObj:
    """Внутренне представление объекта"""

    repo: Repo

    body: bytes

    oid: Oid

    persisted: bool


    def __new__(cls, repo, body):
        oid = Oid.for_object(cls._git_type(), body)
        cache = repo._objects[oid[0]]
        if oid in cache:
            return cache[oid]

        self = super().__new__(cls)
        self.repo = repo
        self.body = body
        self.oid = oid
        self.persisted = False
        cache[oid] = self
        self._parse_body()
        return self

    @classmethod
    def _git_type(cls):
        return cls.__name__.lower()

    def persist(self):
        """Сохраним объект на диск"""
        if self.persisted:
            return self.oid

        self._persist_deps()
        new_oid = self.repo.git(
            "hash-object",
            "--no-filters",
            "-t",
            self._git_type(),
            "-w",
            "--stdin",
            stdin=self.body,
        )

        assert Oid.fromhex(new_oid.decode()) == self.oid
        self.persisted = True
        return self.oid

    def _persist_deps(self):
        pass

    def _parse_body(self):
        pass

    def __eq__(self, other: object) -> bool:
        if isinstance(other, GitObj):
            return self.oid == other.oid
        return False


class Commit(GitObj):
    def _parse_body(self):
        # отделим заголовок от всего коммита
        hdrs, self.message = self.body.split(b"\n\n", maxsplit=1)

        # читаем мету
        self.parent_oids = []
        for hdr in re.split(br"\n(?! )", hdrs):
            key, value = hdr.split(maxsplit=1)
            value = value.replace(b"\n ", b"\n")

            if key == b"tree":
                self.tree_oid = Oid.fromhex(value.decode())
            elif key == b"parent":
                self.parent_oids.append(Oid.fromhex(value.decode()))
            elif key == b"author":
                self.author = Signature(value)
            elif key == b"committer":
                self.committer = Signature(value)

    def get_tree(self):
        """Получим дерево коммита"""
        return self.repo.get_obj(self.tree_oid)

    def parents(self):
        """лист родительских коммитов"""
        return [self.repo.get_obj(parent) for parent in self.parent_oids]

    def prev(self):
        """Получить единственного родителя"""
        if len(self.parents()) != 1:
            raise ValueError(f"У коммита {self.oid} больше одного родителя их - {len(self.parents())} ")
        return self.parents()[0]

    def rebase(self, parent: "Commit") -> "Commit":
        """Новый коммит с теми же изменениями"""
        return rebase(self, parent)

    def summary(self):
        return self.message.split(b"\n", maxsplit=1)[0].decode(errors="replace")

    def update(
        self,
        tree=None,
        parents=None,
        message=None,
        author=None
    ):
        """Создать новый коммит с новыми свойствами"""
        if tree is None:
            tree = self.repo.get_obj(self.tree_oid)
        if parents is None:
            parents = self.parents()
        if message is None:
            message = self.message
        if author is None:
            author = self.author

        # Коммиты не должны дублироваться
        unchanged = (
            tree == self.repo.get_obj(self.tree_oid)
            and parents == self.parents()
            and message == self.message
            and author == self.author
        )
        if unchanged:
            return self
        return self.repo.new_commit(tree, parents, message, author)

    def _persist_deps(self):
        self.get_tree().persist()
        for parent in self.parents():
            parent.persist()

    def __repr__(self) -> str:
        return (
            f"<Commit {self.oid} "
            f"tree={self.tree_oid}, parents={self.parent_oids}, "
            f"author={self.author}, committer={self.committer}>"
        )


class Mode(Enum):
    """Режимы доступа"""

    GITLINK = b"160000"

    SYMLINK = b"120000"

    DIR = b"40000"

    REGULAR = b"100644"

    EXEC = b"100755"

    def is_file(self):
        return self in (Mode.REGULAR, Mode.EXEC)

    def comparable_to(self, other):
        return self == other or (self.is_file() and other.is_file())


class Entry:
    """Тип записи"""
    def __init__(self, repo, mode, oid):
        self.repo = repo
        self.mode = mode
        self.oid = oid

    def tree(self) -> "Tree":
        """Преобразовать в дерево"""
        if self.mode == Mode.DIR:
            return self.repo.get_tree(self.oid)
        return Tree(self.repo, b"")

    def persist(self):
        if self.mode != Mode.GITLINK:
            self.repo.get_obj(self.oid).persist()

    def __repr__(self):
        return f"<Entry {self.mode}, {self.oid}>"

    def __eq__(self, other: object):
        if isinstance(other, Entry):
            return self.mode == other.mode and self.oid == other.oid
        return False


class Tree(GitObj):
    """Тип дерева"""
    def _parse_body(self):
        self.entries = {}
        rest = self.body
        while rest:
            mode, rest = rest.split(b" ", maxsplit=1)
            name, rest = rest.split(b"\0", maxsplit=1)
            entry_oid = Oid(rest[:20])
            rest = rest[20:]
            self.entries[name] = Entry(self.repo, Mode(mode), entry_oid)

    def _persist_deps(self):
        for entry in self.entries.values():
            entry.persist()

    def __repr__(self):
        return f"<Tree {self.oid} ({len(self.entries)} entries)>"


class Reference():  # pylint: disable=unsubscriptable-object
    """Тип ссылки"""



    def __init__(self, obj_type, repo, name):
        self._type = obj_type
        self.name = repo.git("rev-parse", "--symbolic-full-name", name).decode()
        self.repo = repo
        self.refresh()

    def refresh(self):
        """Обновляем объект по ссылке"""
        try:
            obj = self.repo.get_obj(self.name)

            if not isinstance(obj, self._type):
                raise ValueError(
                    f"{type(obj).__name__} {self.name} is not a {self._type.__name__}!"
                )

            self.target = obj
        except MissingObject:
            self.target = None

    def update(self, new, reason):
        """Перенаправляем ссылку на другой объект"""
        new.persist()
        args = ["update-ref", "-m", reason, self.name, str(new.oid)]
        if self.target is not None:
            args.append(str(self.target.oid))

        self.repo.git(*args)
        self.target = new
